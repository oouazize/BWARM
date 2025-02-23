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

def get_latest_folder(sftp, remote_dir_path):
    """Get the most recently created folder from the remote directory"""
    folders = sftp.listdir(remote_dir_path)
    
    # Filter folders that match the expected pattern
    bwarm_folders = [f for f in folders if f.startswith('BWARM_')]
    
    if not bwarm_folders:
        raise Exception("No BWARM folders found in the remote directory")
    
    # Sort folders by the timestamp in their names (last 14 characters contain YYYYMMDDHHmmss)
    latest_folder = max(bwarm_folders, key=lambda x: x[-14:])
    print(f"Latest folder found: {latest_folder}")
    
    return latest_folder

def download_partial_file(sftp, remote_path, local_path, max_size_mb=1):
    """Download only the first max_size_mb MB of a file"""
    max_size_bytes = max_size_mb * 1024 * 1024  # Convert MB to bytes
    
    with sftp.open(remote_path, 'rb') as remote_file:
        with open(local_path, 'wb') as local_file:
            bytes_transferred = 0
            buffer_size = 32768  # 32KB buffer
            
            # Read the header first (first line)
            header = remote_file.readline()
            local_file.write(header)
            bytes_transferred += len(header)
            
            # Read the rest up to max_size_bytes
            while bytes_transferred < max_size_bytes:
                data = remote_file.read(min(buffer_size, max_size_bytes - bytes_transferred))
                if not data:  # EOF
                    break
                local_file.write(data)
                bytes_transferred += len(data)
    
    return bytes_transferred

def download_files_from_sftp(hostname, username, private_key_path, remote_dir_path, local_dir_path):
    """Download partial TSV files from the most recent remote directory"""
    print("\n=== Starting SFTP Connection ===")
    print(f"Connecting to {hostname} as {username}...")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    pkey = paramiko.RSAKey.from_private_key_file(private_key_path)
    
    ssh.connect(hostname=hostname, username=username, pkey=pkey)
    sftp = ssh.open_sftp()
    print("✓ SFTP connection established")
    
    try:
        # Get the latest folder
        print("\n=== Finding Latest Folder ===")
        latest_folder = get_latest_folder(sftp, remote_dir_path)
        full_remote_path = f"{remote_dir_path}/{latest_folder}"
        
        # Create local directory if it doesn't exist
        local_folder_path = os.path.join(local_dir_path, latest_folder)
        pathlib.Path(local_folder_path).mkdir(parents=True, exist_ok=True)
        print(f"✓ Created local directory: {local_folder_path}")
        
        # List all files in the latest remote directory
        print("\n=== Starting File Downloads ===")
        remote_files = sftp.listdir(full_remote_path)
        tsv_files = [f for f in remote_files if f.endswith('.tsv')]
        downloaded_files = []
        print(f"Found {len(tsv_files)} TSV files to download")
        
        for filename in remote_files:
            if filename.endswith('.tsv'):
                remote_path = f"{full_remote_path}/{filename}"
                local_path = os.path.join(local_folder_path, filename)
                
                # Get file size
                file_size = sftp.stat(remote_path).st_size
                print(f"Processing {filename} (Total size: {file_size / (1024*1024*1024):.2f} GB)")
                
                # Download only first 100MB
                bytes_downloaded = download_partial_file(sftp, remote_path, local_path)
                downloaded_files.append(local_path)
                print(f"Downloaded first {bytes_downloaded / (1024*1024):.2f} MB of {filename}")
        
        print(f"\n✓ Downloaded {len(downloaded_files)} files successfully")
        return downloaded_files
    
    finally:
        sftp.close()
        ssh.close()
        print("\n=== Closed SFTP Connection ===")

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
            print(f"Uploaded {len(response['data'])} rows to {table_name}")
            return len(response['data'])
        return 0
    except Exception as e:
        print(f"Error uploading chunk: {str(e)}")
        # Print a sample of the problematic data for debugging
        if chunk:
            print(f"Sample problematic record: {chunk[0]}")
        return 0

def get_table_name_from_filename(filename):
    """Convert filename to table name (e.g., 'sales_data.tsv' -> 'sales_data')"""
    return os.path.splitext(os.path.basename(filename))[0].lower()

def process_and_upload_file(file_path, supabase_url, supabase_key, chunk_size=10000):
    """Process and upload a large file in chunks"""
    table_name = get_table_name_from_filename(file_path)
    total_rows = 0
    chunk_count = 0
    
    print(f"Processing {file_path}")
    
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
            print(f"Uploaded chunk {chunk_count} ({len(chunk)} rows) to {table_name}")
        
        print(f"Completed uploading {total_rows} total rows to {table_name}")
        
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")

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
            print(f"Found {len(downloaded_files)} files in {local_dir}")
        else:
            print(f"Local directory {local_dir} does not exist")

        # Step 2: Process and upload each file in chunks
        # for file_path in downloaded_files:
        #     process_and_upload_file(
        #         file_path=file_path,
        #         supabase_url=config['supabase']['url'],
        #     supabase_key=config['supabase']['key'],
        #     chunk_size=2000
        # )

    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()