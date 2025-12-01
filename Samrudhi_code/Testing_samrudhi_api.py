import requests
import urllib.parse
import pandas as pd
import json
import datetime
import pymysql
import time
import logging
import os
import sys
import re
from pathlib import Path

# Set console encoding to UTF-8 if possible
if sys.stdout.encoding != 'utf-8':
    try:
        # Try to set the output encoding to UTF-8
        # This helps with console output on Windows
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except AttributeError:
        # For older Python versions
        pass

# DB Configuration
try:
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='',
        database='samrudhi_reports',
        connect_timeout=30,
        autocommit=False,
        local_infile=True,
        charset='utf8mb4'  # Ensure UTF-8 support
    )
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    
    # Enable local_infile globally
    cursor.execute("SET GLOBAL local_infile = 1;")
    
except pymysql.Error as e:
    print(f"Database connection failed: {e}")
    exit(1)

# Configure logging
watch = datetime.datetime.now().strftime("%d-%m-%Y-%H-%M-%S")
log_filename = rf"C:\xampp\htdocs\Samrudhi_report\assets\samrudhi_api\log_{watch}.csv"

logging.basicConfig(
    filename=log_filename,
    filemode='w',
    format='%(asctime)s, %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)

# Override print function to also log messages - avoid unicode symbols in console output
original_print = print
def custom_print(*args, **kwargs):
    # Convert Unicode symbols to plain ASCII for console output
    safe_args = []
    for arg in args:
        if isinstance(arg, str):
            # Replace Unicode symbols with ASCII equivalents
            arg = arg.replace("âŒ", "X").replace("âœ…", "OK").replace("âž¡ï¸", "->").replace("ðŸ•'", "TIME:")
        safe_args.append(arg)
    
    # Log the original message (logging can handle Unicode)
    message = " ".join(str(arg) for arg in args)
    logging.info(message)
    
    # Print the safe version to console
    original_print(*safe_args, **kwargs)
print = custom_print

# API details
ip_address = "59.179.22.74"
port = "8080"
username = "test"
password = "test123"
encoded_username = urllib.parse.quote(username)
encoded_password = urllib.parse.quote(password)

# Read State List Excel
try:
    file_path = r"C:\xampp\htdocs\Samrudhi_report\assets\samrudhi_api\State list.xlsx"
    state_list = pd.read_excel(file_path)
    print(f"Successfully loaded state list from {file_path}")
except Exception as e:
    print(f"Error loading state list: {e}")
    connection.close()
    exit(1)

current_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Insert State Data
print("Processing state list...")
for index, row in state_list.iterrows():
    state_names = row['STATE NAME']
    state_codes = row['STATE LGD']

# Setup for data fetch
columns = ['stateName', 'stateCode', 'districtName', 'districtCode', 'blockName', 'blockCode', 
           'gpName', 'gpCode', 'locationname', 'lgdcode', 'status', 'reasonForDown', 
           'neType', 'stateChangeTime']
df = pd.DataFrame(columns=columns)

CHUNK_SIZE = 4096
TIMEOUT = (150, 900)  # (connect timeout, read timeout)

# Function to handle API session
def create_api_session(state_name, state_code):
    login_url = f"http://{ip_address}:{port}/gisinterface/user/login?user={encoded_username}&pswd={encoded_password}"
    
    try:
        login_response = requests.get(login_url, timeout=10)
        login_response.raise_for_status()
        login_data = login_response.json()
        print("Login Response:", login_data)
        
        if login_data.get("status") == "SUCCESS" and "sessionKey" in login_data:
            return login_data["sessionKey"]
        else:
            print("Login failed:", login_data.get("remarks", "Unknown error"))
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None

# Function to fetch data for a state
def fetch_state_data(state_name, state_code, session_key):
    capabilities_url = (
        f"http://{ip_address}:{port}/gisinterface/gis/getBharatNetNeStatus?"
        f"stateName={urllib.parse.quote(state_name)}&stateCode={state_code}&sessionKey={session_key}"
    )
    
    print(f"Fetching data for {state_name}...")
    try:
        with requests.get(capabilities_url, timeout=TIMEOUT, stream=True) as response:
            response.raise_for_status()
            print(f" Server response time: {response.elapsed.total_seconds():.2f} seconds")
            
            data_chunks = []
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    data_chunks.append(chunk)
            
            full_data = b''.join(data_chunks).decode('utf-8')
            
            try:
                capabilities_data = json.loads(full_data)
                if "bharatNetNeDetails" in capabilities_data:
                    return pd.DataFrame(capabilities_data["bharatNetNeDetails"])
                else:
                    print(f"X No 'bharatNetNeDetails' found in response.")
                    return None
            except json.JSONDecodeError:
                print("X Error: Malformed JSON.")
                return None
    except requests.exceptions.RequestException as e:
        print(f"Warning: Request error: {e}")
        return None

# Function to logout from API session
def logout_api_session(session_key):
    logout_url = f"http://{ip_address}:{port}/gisinterface/user/logout?sessionKey={session_key}"
    try:
        logout_response = requests.get(logout_url, timeout=10)
        if logout_response.status_code == 200:
            print("Logout Response:", logout_response.json())
            return True
        else:
            print(f"Logout failed: HTTP {logout_response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"Logout request error: {e}")
        return False

# Function to clean column names for SQL compatibility
def clean_column(column_name):
    """Clean column names to be SQL-safe"""
    # Replace special characters with underscores
    cleaned = re.sub(r'[^\w]', '_', str(column_name))
    # Remove multiple consecutive underscores
    cleaned = re.sub(r'_+', '_', cleaned)
    # Remove leading/trailing underscores
    cleaned = cleaned.strip('_')
    # Ensure it doesn't start with a number
    if cleaned and cleaned[0].isdigit():
        cleaned = 'col_' + cleaned
    return cleaned if cleaned else 'unnamed_column'

# Function to load files from the samrudhi directory
def load_file(file_name: str, row_skip: int = 0, foot_skip: int = 0):
    file_directory = r'C:\xampp\htdocs\Samrudhi_report\assets\samrudhi_api'
    file_path = os.path.join(file_directory, file_name)
    try:
        data = pd.read_csv(file_path, skiprows=row_skip, skipfooter=foot_skip, engine='python')
        print(f"Loaded File: {file_name} with {len(data)} rows")
        return data
    except Exception as e:
        print(f"Error loading file {file_name}: {e}")
        return None

# Function to process GP matching and create matched CSV with SQL insertion
def process_gp_matching(main_csv_filename):
    """Process GP matching between main data and Samriddh_Gram_Panchayat.csv"""
    print(f"\n-> Starting GP matching process for {main_csv_filename}")
    
    try:
        # Load files
        samrudhi = load_file('Samriddh_Gram_Panchayat.csv')
        daily = load_file(main_csv_filename)
        
        if samrudhi is None or daily is None:
            print("X Error: Could not load required files for GP matching")
            return False
        
        print(f" Samrudhi GP data: {len(samrudhi)} rows")
        print(f" Daily data: {len(daily)} rows")
        
        # Clean codes - convert to numeric
        samrudhi['GP_Code'] = pd.to_numeric(samrudhi['GP_Code'], errors='coerce')
        daily['lgdcode'] = pd.to_numeric(daily['lgdcode'], errors='coerce')

        # Drop nulls after conversion
        samrudhi_clean = samrudhi.dropna(subset=['GP_Code'])
        daily_clean = daily.dropna(subset=['lgdcode'])

        # Convert to integer (remove .0 problem)
        samrudhi_clean['GP_Code'] = samrudhi_clean['GP_Code'].astype(int)
        daily_clean['lgdcode'] = daily_clean['lgdcode'].astype(int)
        
        print(f" After cleaning - Samrudhi: {len(samrudhi_clean)} rows, Daily: {len(daily_clean)} rows")
        
        # Remove duplicates like SQL GROUP BY
        daily_unique = daily_clean.drop_duplicates(subset=['lgdcode'])
        print(f" Daily unique records: {len(daily_unique)} rows")
        
        # Merge - inner join to get only matched records
        matched = pd.merge(
            daily_unique,
            samrudhi_clean[['GP_Code']],
            left_on='lgdcode',
            right_on='GP_Code',
            how='inner'
        )
        
        print(f" Matched records: {len(matched)} rows")
        
        if len(matched) == 0:
            print("X Warning: No matching records found between lgdcode and GP_Code")
            return False
        
        # Create timestamped table name and file path
        samrudhi_sql_time = datetime.datetime.now().strftime("%d_%m_%Y_%H_%M_%S")
        samrudhi_table = f"matched_gp_{samrudhi_sql_time}"
        samrudhi_csv_path = rf"C:\xampp\htdocs\Samrudhi_report\assets\samrudhi_api\{samrudhi_table}.csv"
        
        # Create a copy and clean column names
        matched_output = matched.copy()
        matched_output.columns = [clean_column(col) for col in matched_output.columns]
        samrudhi_column_names = matched_output.columns.tolist()
        
        # Save matched data to CSV
        matched_output.to_csv(samrudhi_csv_path, index=False)
        print(f" Matched GP data saved: {samrudhi_csv_path}")
        
        # Prepare SQL queries
        samrudhi_columns_sql = ",\n    ".join([f"`{col}` TEXT" for col in samrudhi_column_names])
        samrudhi_create_query = f"""
CREATE TABLE IF NOT EXISTS `{samrudhi_table}` (
    `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    {samrudhi_columns_sql}
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""
        
        # Format path for SQL (use forward slashes)
        samrudhi_csv_path_sql = samrudhi_csv_path.replace("\\", "\\\\")
        
        samrudhi_load_query = f"""
LOAD DATA LOCAL INFILE '{samrudhi_csv_path_sql}'
INTO TABLE `{samrudhi_table}`
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\\n'
IGNORE 1 LINES
({', '.join([f"`{col}`" for col in samrudhi_column_names])});
"""
        
        # Execute SQL operations
        try:
            # Create new connection for this operation
            conn_gp = pymysql.connect(
                host='localhost',
                user='root',
                password='',
                database='samrudhi_reports',
                local_infile=True,
                charset='utf8mb4'
            )
            cur_gp = conn_gp.cursor()
            
            # Create table
            cur_gp.execute(samrudhi_create_query)
            print(f" Table `{samrudhi_table}` created successfully.")
            
            # Load data
            cur_gp.execute(samrudhi_load_query)
            conn_gp.commit()
            
            # Verify insertion
            cur_gp.execute(f"SELECT COUNT(*) as count FROM `{samrudhi_table}`")
            inserted_count = cur_gp.fetchone()[0]
            print(f" Matched GP MySQL table created and {inserted_count} records inserted successfully.")
            
            return True
            
        except Exception as e:
            print(f"X Matched GP SQL Error: {e}")
            if 'conn_gp' in locals():
                conn_gp.rollback()
            return False
        finally:
            if 'cur_gp' in locals():
                cur_gp.close()
            if 'conn_gp' in locals():
                conn_gp.close()
                
    except Exception as e:
        print(f"X Error in GP matching process: {e}")
        return False

# Optimized function to create table and load data
def create_table_and_load_csv(csv_path):
    try:
        # Create a valid table name 
        table_name = os.path.basename(csv_path).split('.')[0].replace('-', '_')
        print(f"Creating table: {table_name}")
        
        # Load the CSV data into a DataFrame - just to get column names
        csv_df = pd.read_csv(csv_path)
        total_rows = len(csv_df)
        print(f" CSV loaded with {total_rows} rows and {len(csv_df.columns)} columns")
        
        # Get column names from DataFrame
        column_names = csv_df.columns.tolist()
        
        # Create table with column names from CSV
        columns_list = ', '.join([f'`{col}` TEXT' for col in column_names])
        
        # Add auto-increment ID column
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                {columns_list}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
        
        start_time = time.time()
        
        # Create table
        cursor.execute(create_table_query)
        connection.commit()
        print(f" Table `{table_name}` created successfully.")
        
        # Ensure correct path formatting for Windows
        formatted_path = csv_path.replace('\\', '\\\\')
        
        # Load data using LOAD DATA LOCAL INFILE - in its own transaction
        load_data_query = f"""
            LOAD DATA LOCAL INFILE '{formatted_path}'
            INTO TABLE `{table_name}`
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"' 
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            ({', '.join([f"`{col}`" for col in column_names])});
        """
        
        # Execute load data query
        cursor.execute(load_data_query)
        
        # Commit the data loading transaction
        connection.commit()
        
        # Verify how many rows were inserted
        cursor.execute(f"SELECT COUNT(*) as row_count FROM `{table_name}`")
        inserted_rows = cursor.fetchone()['row_count']
        print(f" Loaded {inserted_rows} rows out of {total_rows} expected rows.")
        
        if inserted_rows < total_rows:
            print(f"X Warning: Not all rows were inserted. Expected {total_rows}, got {inserted_rows}")
        
        # Add indexes before the update operation
        print(" Adding indexes on 'lgdcode' and 'status' columns...")
        index_queries = [
            f"ALTER TABLE `{table_name}` ADD INDEX idx_lgdcode (lgdcode);",
            f"ALTER TABLE `{table_name}` ADD INDEX idx_status (status);"
        ]
        
        for query in index_queries:
            try:
                cursor.execute(query)
                connection.commit()
                print(f" Index added successfully")
            except Exception as e:
                print(f" Index creation warning: {e}")
        
        # Run the update to trim leading zeros from `lgdcode` - in a separate transaction
        print(" Running update to trim leading zeros from lgdcode...")
        update_query = f"""
            UPDATE `{table_name}`
            SET `lgdcode` = TRIM(LEADING '0' FROM `lgdcode`)
            WHERE `lgdcode` LIKE '0%';
        """
        
        cursor.execute(update_query)
        updated_rows = cursor.rowcount
        connection.commit()
        print(f" Updated {updated_rows} rows with leading zeros in lgdcode.")
        
        end_time = time.time()
        print(f" Data processing for `{table_name}` completed successfully.")
        print(f"TIME: Time taken: {end_time - start_time:.2f} seconds.")
        return True
        
    except Exception as e:
        print(f"X Error during table creation or data loading: {e}")
        connection.rollback()
        return False

# Process each state
for _, row in state_list.iterrows():
    state_name = row['STATE NAME']
    state_code = row['STATE LGD']
    
    print(f"\nFetching data for {state_name} ({state_code})...")
    
    session_key = create_api_session(state_name, state_code)
    if not session_key:
        continue
    
    print(f"Session Key: {session_key}")
    
    new_df = fetch_state_data(state_name, state_code, session_key)
    if new_df is not None:
        df = pd.concat([df, new_df], ignore_index=True)
        print(f" Data fetched successfully for {state_name}!")
    
    logout_api_session(session_key)

# Save the DataFrame to CSV
watch1 = datetime.datetime.now().strftime("%d-%m-%Y-%H-%M-%S")
saved_csv_path = rf"C:\xampp\htdocs\Samrudhi_report\assets\samrudhi_api\{watch1}.csv"

df.to_csv(saved_csv_path, index=None)
print(f" Data saved to {saved_csv_path}")

# Process the CSV data
try:
    print(f"\n-> Starting CSV to Table process")
    main_table_success = create_table_and_load_csv(saved_csv_path)
    
    if main_table_success:
        print(f"\n-> Starting GP matching process")
        # Extract just the filename for the matching process
        main_csv_filename = os.path.basename(saved_csv_path)
        gp_matching_success = process_gp_matching(main_csv_filename)
        
        if gp_matching_success:
            print(f" GP matching and table creation completed successfully!")
        else:
            print(f"X GP matching process failed")
    else:
        print(f"X Main table creation failed - skipping GP matching")
    
except Exception as e:
    print(f"X Critical error during processing: {e}")
    connection.rollback()
finally:
    # Always close database connections
    cursor.close()
    connection.close()
    print("Database connection closed")