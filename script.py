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

def display_local_files_info(local_dir_path):
    """Display information about all files in the local directory"""
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
            except Exception as e:
                logger.error(f"Error getting stats for local file {filename}: {str(e)}")
                
        total_size_gb = total_size_bytes / (1024*1024*1024)
        if total_size_gb >= 1:
            logger.info(f"\nTotal size of all local files: {total_size_gb:.2f} GB")
        else:
            total_size_mb = total_size_bytes / (1024*1024)
            logger.info(f"\nTotal size of all local files: {total_size_mb:.2f} MB")
    except Exception as e:
        logger.error(f"Error listing local directory {local_dir_path}: {str(e)}")

def download_file(sftp, remote_path, local_path):
    """Download the entire file"""
    logger.info(f"Downloading {os.path.basename(remote_path)}...")
    sftp.get(remote_path, local_path)
    file_size = os.path.getsize(local_path)
    logger.info(f"✓ Downloaded {file_size / (1024*1024):.2f} MB")
    return file_size

def download_files_from_sftp(hostname, username, private_key_path, remote_dir_path, local_dir_path):
    """Download TSV files from the most recent remote directory"""
    logger.info("\n=== Starting SFTP Connection ===")
    logger.info(f"Connecting to {hostname} as {username}...")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    pkey = paramiko.RSAKey.from_private_key_file(private_key_path)
    
    ssh.connect(hostname=hostname, username=username, pkey=pkey)
    sftp = ssh.open_sftp()
    logger.info("✓ SFTP connection established")
    
    try:
        # Get the latest folder
        logger.info("\n=== Finding Latest Folder ===")
        latest_folder = get_latest_folder(sftp, remote_dir_path)
        full_remote_path = f"{remote_dir_path}/{latest_folder}"
        
        # Display information about remote files before downloading
        display_remote_files_info(sftp, full_remote_path)
        
        # Display information about existing local files 
        display_local_files_info(local_dir_path)
        
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
                
                # Download the entire file
                bytes_downloaded = download_file(sftp, remote_path, local_path)
                downloaded_files.append(local_path)
        
        logger.info(f"\n✓ Downloaded {len(downloaded_files)} files successfully")
        return downloaded_files
    
    finally:
        sftp.close()
        ssh.close()
        logger.info("\n=== Closed SFTP Connection ===")

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
            logger.info(f"Uploaded {len(response['data'])} rows to {table_name}")
            return len(response['data'])
        return 0
    except Exception as e:
        logger.error(f"Error uploading chunk: {str(e)}")
        # logger.info a sample of the problematic data for debugging
        if chunk:
            logger.info(f"Sample problematic record: {chunk[0]}")
        return 0

def get_table_name_from_filename(filename):
    """Convert filename to table name (e.g., 'sales_data.tsv' -> 'sales_data')"""
    return os.path.splitext(os.path.basename(filename))[0].lower()

def process_and_upload_file(file_path, supabase_url, supabase_key, chunk_size=10000):
    """Process and upload a large file in chunks"""
    table_name = get_table_name_from_filename(file_path)
    total_rows = 0
    chunk_count = 0
    
    logger.info(f"Processing {file_path}")
    
    try:
        for chunk in process_large_file(file_path, chunk_size):
            chunk_count += 1

            rows_uploaded = upload_chunk_to_supabase(
                chunk=chunk,
                supabase_url=supabase_url,
                supabase_key=supabase_key,
                table_name=table_name
            )
            total_rows += rows_uploaded
            logger.info(f"Uploaded chunk {chunk_count} ({len(chunk)} rows) to {table_name}")
        
        logger.info(f"Completed uploading {total_rows} total rows to {table_name}")
        
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {str(e)}")

def main():
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
        # Step 1: Download files from the latest SFTP folder
        downloaded_files = download_files_from_sftp(
            hostname=config['sftp']['hostname'],
            username=config['sftp']['username'],
            private_key_path=config['sftp']['private_key_path'],
            remote_dir_path=config['sftp']['remote_dir_path'],
            local_dir_path=config['sftp']['local_dir_path']
        )
        
        # Get list of files from local directory
        local_dir = config['sftp']['local_dir_path']
        
        if os.path.exists(local_dir):
            for file in os.listdir(local_dir):
                file_path = os.path.join(local_dir, file)
                if os.path.isfile(file_path):
                    downloaded_files.append(file_path)
            logger.info(f"Found {len(downloaded_files)} files in {local_dir}")
        else:
            logger.info(f"Local directory {local_dir} does not exist")

        # Step 2: Process and upload each file in chunks
        # for file_path in downloaded_files:
        #     process_and_upload_file(
        #         file_path=file_path,
        #         supabase_url=config['supabase']['url'],
        #     supabase_key=config['supabase']['key'],
        #     chunk_size=2000
        # )

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()