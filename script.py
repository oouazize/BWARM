# main.py

import os
import csv
import paramiko
from supabase import create_client, Client
from dotenv import load_dotenv
import pathlib
from datetime import datetime
import pandas as pd
from column_mappings import get_column_names, get_array_columns
from loguru import logger
import sys
import time
import concurrent.futures
import json
import re
import argparse

# Configure Loguru
logger.remove()  # Remove default logger

# Log to a file
logger.add("/var/log/myscript.log", rotation="10GB", level="INFO", enqueue=True)

# Log to systemd (stdout)
logger.add(sys.stdout, format="{time} {level} {message}", level="INFO")

# Log to systemd (stderr for errors)
logger.add(sys.stderr, format="{time} {level} {message}", level="ERROR")

logger.info("Script started successfully!")

def get_latest_folder(sftp, remote_dir_path):
    """Get the most recently created folder from the remote directory"""
    folders = sftp.listdir(remote_dir_path)
    
    # Filter folders that match the expected pattern
    bwarm_folders = [f for f in folders if f.startswith('BWARM_')]
    
    if not bwarm_folders:
        raise Exception("No BWARM folders found in the remote directory")
    
    # Sort folders by the timestamp in their names (last 14 characters contain YYYYMMDDHHmmss)
    latest_folder = max(bwarm_folders, key=lambda x: x[-14:])
    logger.info(f"Latest folder found: {latest_folder}")
    
    return latest_folder

def display_remote_files_info(sftp, remote_path):
    """Display information about all files in the remote directory"""
    logger.info("\n=== Remote Files Information ===")
    try:
        remote_files = sftp.listdir(remote_path)
        logger.info(f"Found {len(remote_files)} files in remote directory")
        
        total_size_bytes = 0
        for filename in remote_files:
            file_path = f"{remote_path}/{filename}"
            try:
                file_stat = sftp.stat(file_path)
                file_size_bytes = file_stat.st_size
                total_size_bytes += file_size_bytes
                file_size_mb = file_size_bytes / (1024*1024)
                file_size_gb = file_size_bytes / (1024*1024*1024)
                
                if file_size_gb >= 1:
                    logger.info(f"Remote file: {filename} - Size: {file_size_gb:.2f} GB")
                else:
                    logger.info(f"Remote file: {filename} - Size: {file_size_mb:.2f} MB")
            except Exception as e:
                logger.error(f"Error getting stats for remote file {filename}: {str(e)}")
                
        total_size_gb = total_size_bytes / (1024*1024*1024)
        if total_size_gb >= 1:
            logger.info(f"\nTotal size of all remote files: {total_size_gb:.2f} GB")
        else:
            total_size_mb = total_size_bytes / (1024*1024)
            logger.info(f"\nTotal size of all remote files: {total_size_mb:.2f} MB")
    except Exception as e:
        logger.error(f"Error listing remote directory {remote_path}: {str(e)}")

def display_local_files_info(local_dir_path, delete_files=True):
    """Display information about all files in the local directory and optionally delete them"""
    logger.info("\n=== Local Files Information ===")
    try:
        if not os.path.exists(local_dir_path):
            logger.info(f"Local directory {local_dir_path} does not exist yet")
            return
            
        local_files = [f for f in os.listdir(local_dir_path) if os.path.isfile(os.path.join(local_dir_path, f))]
        logger.info(f"Found {len(local_files)} files in local directory")
        
        total_size_bytes = 0
        for filename in local_files:
            file_path = os.path.join(local_dir_path, filename)
            try:
                file_size_bytes = os.path.getsize(file_path)
                total_size_bytes += file_size_bytes
                file_size_mb = file_size_bytes / (1024*1024)
                file_size_gb = file_size_bytes / (1024*1024*1024)
                
                if file_size_gb >= 1:
                    logger.info(f"Local file: {filename} - Size: {file_size_gb:.2f} GB")
                else:
                    logger.info(f"Local file: {filename} - Size: {file_size_mb:.2f} MB")
                    
                # Delete the file if requested
                if delete_files:
                    os.remove(file_path)
                    logger.info(f"Deleted file: {filename}")
                    
            except Exception as e:
                logger.error(f"Error processing local file {filename}: {str(e)}")
                
        total_size_gb = total_size_bytes / (1024*1024*1024)
        if total_size_gb >= 1:
            logger.info(f"\nTotal size of all local files: {total_size_gb:.2f} GB")
        else:
            total_size_mb = total_size_bytes / (1024*1024)
            logger.info(f"\nTotal size of all local files: {total_size_mb:.2f} MB")
            
        if delete_files and local_files:
            logger.info(f"All {len(local_files)} local files have been deleted")
    except Exception as e:
        logger.error(f"Error listing local directory {local_dir_path}: {str(e)}")

def download_file(sftp, remote_path, local_path, size_limit=None):
    """
    Download file from remote server
    If size_limit is provided (in bytes), download only up to that size
    """
    logger.info(f"Downloading {os.path.basename(remote_path)}...")
    
    if size_limit is not None:
        # Download only a portion of the file
        with open(local_path, 'wb') as local_file:
            with sftp.open(remote_path, 'rb') as remote_file:
                data = remote_file.read(size_limit)
                local_file.write(data)
        logger.info(f"✓ Downloaded first {size_limit/(1024*1024):.2f} MB (limited download)")
    else:
        # Download the entire file
        sftp.get(remote_path, local_path)
        
    file_size = os.path.getsize(local_path)
    file_size_gb = file_size / (1024*1024*1024)
    # Display in GB format
    logger.info(f"✓ Downloaded {file_size_gb:.4f} GB")
    return file_size

def download_files_from_sftp(hostname, username, private_key_path, remote_dir_path, local_dir_path, max_retries=3, size_limit=None):
    """Download TSV files from the most recent remote directory"""
    logger.info("\n=== Starting SFTP Connection ===")
    logger.info(f"Connecting to {hostname} as {username}...")
    
    retry_count = 0
    while retry_count < max_retries:
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            pkey = paramiko.RSAKey.from_private_key_file(private_key_path)
            
            # Add connection timeout and keep-alive options
            ssh.connect(
                hostname=hostname, 
                username=username, 
                pkey=pkey,
                timeout=30,
                banner_timeout=60
            )
            
            # Set keep-alive packets
            transport = ssh.get_transport()
            transport.set_keepalive(30)  # Send keep-alive every 30 seconds
            
            sftp = ssh.open_sftp()
            logger.info("✓ SFTP connection established")
            
            try:
                # Get the latest folder
                logger.info("\n=== Finding Latest Folder ===")
                latest_folder = get_latest_folder(sftp, remote_dir_path)
                full_remote_path = f"{remote_dir_path}/{latest_folder}"
                
                # Display information about remote files before downloading
                display_remote_files_info(sftp, full_remote_path)
                
                # Display information about existing local files and delete them
                display_local_files_info(local_dir_path, delete_files=True)
                
                # Ensure the local directory exists after possible deletion
                os.makedirs(local_dir_path, exist_ok=True)
                
                # List all files in the latest remote directory
                logger.info("\n=== Starting File Downloads ===")
                remote_files = sftp.listdir(full_remote_path)
                tsv_files = [f for f in remote_files if f.endswith('.tsv')]
                downloaded_files = []
                logger.info(f"Found {len(tsv_files)} TSV files to download")
                
                for filename in remote_files:
                    if filename.endswith('.tsv'):
                        remote_path = f"{full_remote_path}/{filename}"
                        local_path = os.path.join(local_dir_path, filename)
                        
                        # Get file size
                        file_size = sftp.stat(remote_path).st_size
                        logger.info(f"Processing {filename} (Total size: {file_size / (1024*1024*1024):.2f} GB)")
                        
                        # Download with retry mechanism
                        file_retry = 0
                        while file_retry < 3:  # Retry up to 3 times per file
                            try:
                                bytes_downloaded = download_file(sftp, remote_path, local_path, size_limit)
                                downloaded_files.append(local_path)
                                break  # Success, exit retry loop
                            except Exception as e:
                                file_retry += 1
                                logger.error(f"Error downloading {filename} (attempt {file_retry}): {str(e)}")
                                if file_retry >= 3:
                                    raise  # Re-raise after max retries
                                logger.info(f"Retrying download in 5 seconds...")
                                time.sleep(5)  # Wait before retry
                
                logger.info(f"\n✓ Downloaded {len(downloaded_files)} files successfully")
                return downloaded_files
                
            finally:
                sftp.close()
                ssh.close()
                logger.info("\n=== Closed SFTP Connection ===")
                
            # If we got here without exceptions, break out of the retry loop
            break
            
        except paramiko.SSHException as e:
            retry_count += 1
            logger.error(f"SSH connection error (attempt {retry_count}/{max_retries}): {str(e)}")
            if retry_count >= max_retries:
                logger.info("Maximum retries reached. Giving up on SFTP connection.")
                raise
            wait_time = 2 ** retry_count  # Exponential backoff
            logger.info(f"Retrying connection in {wait_time} seconds...")
            time.sleep(wait_time)
        except Exception as e:
            logger.error(f"Unexpected error in SFTP connection: {str(e)}")
            raise
    
    # This should never be reached if the function runs properly
    raise Exception("Failed to establish SFTP connection after multiple attempts")

def process_large_file(file_path, chunk_size=10000):
    """Generator function to read large TSV files in chunks with NaN handling"""
    # Get table name and corresponding column names
    table_name = get_table_name_from_filename(file_path)
    column_names = get_column_names(table_name)
    array_columns = get_array_columns(table_name)
    
    if not column_names:
        raise ValueError(f"No column mapping found for table {table_name}")
    
    # Read the file with the specified column names
    for chunk in pd.read_csv(
        file_path, 
        sep='\t', 
        chunksize=chunk_size,
        names=column_names,  # Use predefined column names
        header=None  # Treat first row as data, not header
    ):
        # Replace NaN/inf values with None (null in JSON)
        chunk = chunk.replace({
            pd.NA: None,
            pd.NaT: None,
            float('inf'): None,
            float('-inf'): None,
            'nan': None,
            float('nan'): None
        })
        
        # Convert DataFrame chunk to records
        records = chunk.where(chunk.notna(), None).to_dict('records')
        
        # Post-process records to handle array fields
        for record in records:
            for array_column in array_columns:
                if array_column in record and record[array_column] is not None:
                    if isinstance(record[array_column], bool):
                        record[array_column] = []
                    elif isinstance(record[array_column], (str, int, float)):
                        record[array_column] = [record[array_column]]
        
        yield records

def upload_chunk_to_supabase(chunk, supabase_url, supabase_key, table_name):
    """Upload a chunk of rows to Supabase"""
    supabase: Client = create_client(supabase_url, supabase_key)
    try:
        # Filter out any remaining None values from dictionaries
        cleaned_chunk = [{k: v for k, v in record.items() if v is not None} for record in chunk]
        
        response = supabase.table(table_name).upsert(cleaned_chunk).execute()
        if 'data' in response:
            return len(cleaned_chunk)  # Return the number of rows we attempted to upload
        return 1
    except Exception as e:
        # Re-raise the exception so the parent function can handle it with its retry logic
        raise

def get_table_name_from_filename(filename):
    """Convert filename to table name (e.g., 'sales_data.tsv' -> 'sales_data')"""
    return os.path.splitext(os.path.basename(filename))[0].lower()

def process_and_upload_file(file_path, supabase_url, supabase_key, chunk_size=10000, max_retries=3):
    """Process and upload a large file in chunks with retry logic and progress display"""
    table_name = get_table_name_from_filename(file_path)
    total_rows = 0
    total_rows_processed = 0
    chunk_count = 0
    start_time = time.time()
    
    # Keep track of problematic records
    problematic_records = []
    
    logger.info(f"📂 Processing {os.path.basename(file_path)}")
    
    # Count total rows for progress calculation
    try:
        total_file_rows = sum(1 for _ in open(file_path, 'r', encoding='utf-8'))
        logger.info(f"📊 Total rows in file: {total_file_rows:,}")
    except Exception as e:
        logger.info(f"⚠️ Could not count total rows: {str(e)}")
        total_file_rows = 0
    
    def upload_with_retry(records, max_attempts=3):
        """Try to upload records with retry logic for transient errors"""
        for attempt in range(max_attempts):
            try:
                if not records:  # Skip empty record lists
                    return True
                    
                return upload_chunk_to_supabase(
                    chunk=records,
                    supabase_url=supabase_url,
                    supabase_key=supabase_key,
                    table_name=table_name
                ) > 0
            except Exception as e:
                error_str = str(e)
                # Only retry for network errors, not data errors
                if ("[Errno 54]" in error_str or "StreamReset" in error_str) and attempt < max_attempts - 1:
                    wait_time = 2 ** attempt
                    logger.error(f"⚠️ Network error, retrying in {wait_time}s... (attempt {attempt+1}/{max_attempts})")
                    time.sleep(wait_time)
                else:
                    # For data errors or final attempt, return False to indicate failure
                    return False
        return False
    
    def binary_search_upload(records, isolation_threshold=5):
        """
        Use binary search approach to isolate and identify problematic records.
        Returns the number of successfully uploaded records.
        """
        nonlocal total_rows
        
        # Base case: empty list
        if not records:
            return 0
            
        # Base case: if we're down to a small number of records, try them one by one
        if len(records) <= isolation_threshold:
            success_count = 0
            for i, record in enumerate(records):
                if upload_with_retry([record]):
                    success_count += 1
                    total_rows += 1
                else:
                    problematic_records.append(record)
            return success_count
            
        # Try to upload the whole chunk first
        if upload_with_retry(records):
            total_rows += len(records)
            return len(records)
            
        # If failed, split and try each half separately
        mid = len(records) // 2
        first_half = records[:mid]
        second_half = records[mid:]
        
        # Process each half
        success_first = binary_search_upload(first_half, isolation_threshold)
        success_second = binary_search_upload(second_half, isolation_threshold)
        
        return success_first + success_second
    
    # Process the file
    try:
        # Track the last reported milestone
        last_milestone = 0
        milestones = [15, 30, 45, 60, 75, 90, 100]
        
        for chunk in process_large_file(file_path, chunk_size):
            chunk_count += 1
            chunk_start_time = time.time()
            
            logger.info(f"🔄 Processing chunk {chunk_count} of {table_name} ({len(chunk)} rows)")
            
            # Use binary search approach to upload as many records as possible
            successful_records = binary_search_upload(chunk)
            total_rows_processed += len(chunk)
            
            # Calculate progress
            chunk_elapsed = time.time() - chunk_start_time
            speed = successful_records / chunk_elapsed if chunk_elapsed > 0 else 0
            
            if total_file_rows > 0:
                progress_pct = min(100, (total_rows_processed / total_file_rows) * 100)
                
                # Check if we've reached a new milestone
                current_milestone = next((m for m in milestones if m > last_milestone and progress_pct >= m), None)
                if current_milestone:
                    logger.info(f"📊 {current_milestone}% complete - {table_name} - Chunk {chunk_count}: {successful_records}/{len(chunk)} successful ({speed:.1f} r/s)")
                    last_milestone = current_milestone
            else:
                # If total_file_rows is unknown, just show chunk information
                logger.info(f"{table_name} - Chunk {chunk_count}: {successful_records}/{len(chunk)} successful ({speed:.1f} r/s)")
        
        # Final statistics
        total_elapsed = time.time() - start_time
        avg_speed = total_rows / total_elapsed if total_elapsed > 0 else 0
        success_rate = (total_rows / total_rows_processed) * 100 if total_rows_processed > 0 else 0
        
        logger.info(f"\n✅ Completed {table_name}: {total_rows:,}/{total_rows_processed:,} rows ({success_rate:.1f}% success) in {format_time(total_elapsed)} "
              f"(avg: {avg_speed:.1f} rows/sec)")
        
        # Report problematic records
        if problematic_records:
            logger.info(f"⚠️ {len(problematic_records)} records could not be uploaded due to data issues")
            
            # Save problematic records to a file for later inspection
            error_file = f"{os.path.splitext(file_path)[0]}_errors.json"
            try:
                with open(error_file, 'w', encoding='utf-8') as f:
                    json.dump(problematic_records[:1000], f, indent=2)  # Save up to 1000 for investigation
                logger.info(f"💾 Problematic records saved to {error_file}")
                
                # Try to logger.info some diagnostic info about the first few problematic records
                logger.info(f"Sample of problematic records:")
                for i, record in enumerate(problematic_records[:3]):
                    logger.info(f"  Record {i+1}: {json.dumps(record)[:200]}...")
            except Exception as save_err:
                logger.error(f"Could not save error records: {str(save_err)}")
        
    except Exception as e:
        logger.error(f"❌ Error processing file {os.path.basename(file_path)}: {str(e)}")

def format_time(seconds):
    """Format time in seconds to a readable string"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds // 60
        sec = seconds % 60
        return f"{int(minutes)}m {int(sec)}s"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        sec = seconds % 60
        return f"{int(hours)}h {int(minutes)}m {int(sec)}s"

def get_processing_order():
    """Returns the ordered list of tables to process based on dependencies"""
    # Define the processing steps in order with exact file names
    processing_steps = {
        1: ["works.tsv", "parties.tsv", "releases.tsv"],
        2: ["releaseidentifiers.tsv", "recordings.tsv", "workidentifiers.tsv", "workalternativetitles.tsv", "workrightshares.tsv"],
        3: ["recordingalternativetitles.tsv", "worksrecordings.tsv", "unclaimedworkrightshares.tsv"]
    }
    
    # Flatten into a single ordered list of base filenames
    ordered_tables = []
    for step in sorted(processing_steps.keys()):
        ordered_tables.extend(processing_steps[step])
    
    return ordered_tables, processing_steps

def process_step_in_parallel(files, supabase_url, supabase_key, max_workers=4):
    """Process multiple files in parallel with a limit on concurrent workers and progress tracking"""
    total_files = len(files)
    completed_files = 0
    start_time = time.time()
    
    logger.info(f"\n=== Starting parallel processing of {total_files} files with {max_workers} workers ===")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Map files to future objects
        future_to_file = {
            executor.submit(
                process_and_upload_file,
                file_path=file_path,
                supabase_url=supabase_url,
                supabase_key=supabase_key,
                chunk_size=1000,
            ): file_path for file_path in files
        }
        
        # Process completed futures
        for future in concurrent.futures.as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                future.result()
                completed_files += 1
                
                # Calculate overall progress
                progress_pct = (completed_files / total_files) * 100
                elapsed = time.time() - start_time
                
                # Progress bar for overall completion
                bar_length = 30
                filled_length = int(bar_length * progress_pct / 100)
                bar = '█' * filled_length + '░' * (bar_length - filled_length)
                
                logger.info(f"\n🔄 OVERALL PROGRESS: [{bar}] {progress_pct:.1f}% | "
                      f"Completed: {completed_files}/{total_files} files | "
                      f"Elapsed: {format_time(elapsed)}")
                
            except Exception as e:
                logger.error(f"❌ Error processing {os.path.basename(file_path)}: {str(e)}")
    
    # Final statistics
    total_elapsed = time.time() - start_time
    logger.info(f"\n✅ STEP COMPLETED: Processed {completed_files}/{total_files} files in {format_time(total_elapsed)}")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process and upload data files to Supabase.')
    parser.add_argument('--row-by-row', action='store_true', help='Process each row individually instead of in batches')
    args = parser.parse_args()
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment variables
    config = {
        'sftp': {
            'hostname': os.getenv('SFTP_HOSTNAME'),
            'username': os.getenv('SFTP_USERNAME'),
            'private_key_path': os.getenv('PRIVATE_KEY_PATH'),
            'remote_dir_path': os.getenv('REMOTE_DIR_PATH'),
            'local_dir_path': os.getenv('LOCAL_DIR_PATH'),
        },
        'supabase': {
            'url': os.getenv('SUPABASE_URL'),
            'key': os.getenv('SUPABASE_KEY'),
        }
    }

    try:

        # downloaded_files = download_files_from_sftp(
        #     hostname=config['sftp']['hostname'],
        #     username=config['sftp']['username'],
        #     private_key_path=config['sftp']['private_key_path'],
        #     remote_dir_path=config['sftp']['remote_dir_path'],
        #     local_dir_path=config['sftp']['local_dir_path'],
        #     max_retries=5,  # Add retry parameter
        # )
        # Get list of files from local directory
        local_dir = config['sftp']['local_dir_path']
        available_files = {}  # Use a dictionary to map filename -> full path
        
        logger.info(f"\n=== Reading files from local directory: {local_dir} ===")
        if os.path.exists(local_dir):
            for file in os.listdir(local_dir):
                file_path = os.path.join(local_dir, file)
                if os.path.isfile(file_path) and file.endswith('.tsv'):
                    available_files[file] = file_path  # Store by filename for exact matching
                    logger.info(f"Found file: {file}")
            logger.info(f"Found {len(available_files)} TSV files in {local_dir}")
        else:
            logger.info(f"Local directory {local_dir} does not exist")
            return

        # Get ordered list of tables and processing steps
        ordered_tables, processing_steps = get_processing_order()
        
        # Process files in the correct order by step
        for step, file_list in sorted(processing_steps.items()):
            logger.info(f"\n=== Processing Step {step} ===")
            step_files = []
            
            # Find files that match exactly with this step's file list
            for filename in file_list:
                if filename in available_files:
                    step_files.append(available_files[filename])
                else:
                    logger.info(f"Warning: File {filename} not found in directory")
            
            if not step_files:
                logger.info(f"No files found for step {step}: {file_list}")
                continue
                
            logger.info(f"Processing {len(step_files)} files for step {step} in parallel:")
            for file_path in step_files:
                logger.info(f"  - {os.path.basename(file_path)}")
            
            # Process this step's files in parallel
            process_step_in_parallel(
                files=step_files,
                supabase_url=config['supabase']['url'],
                supabase_key=config['supabase']['key'],
                max_workers=4,  # Adjust based on your server capacity
            )
            
            logger.info(f"Completed processing step {step}")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        import traceback
        traceback.logger.info_exc()

if __name__ == "__main__":
    main()