import os 
import glob
import csv
from cassandra.cluster import Cluster
from sql_queries import *
import pandas as pd
from cassandra.cluster import Cluster

def get_file_path_list(folder_name):
    '''
    Returns list of filepaths to files whose data is to be processed.

    '''

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, folder_name)


    all_files = []

    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(data_dir):
        files = glob.glob(os.path.join(root, '*.csv'))
        for f in files:
            all_files.append(os.path.abspath(f))

    
    return all_files








# def merge_to_one_csv(file_path_list, output_file_name, column_list):
#     '''
#     Processing the files to create the data file csv that will be used for Apache Casssandra tables
#     '''

#     # initiating an empty list for all rows that will be generated the files
#     full_data_rows_list = [] 

#     # Define the columns you want to extract and write to the output CSV
#     target_columns = column_list
    
    
#     # for every filepath in the file path list, iterate through and read csv file
#     for f in file_path_list: 
#         with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
#             csvreader = csv.reader(csvfile) 
#             next(csvreader)
            
#     # extracting each data row one by one and append it        
#             for line in csvreader:
#                 #print(line)
#                 full_data_rows_list.append(line) 


#     # Print the total number of rows
#     print(len(full_data_rows_list))

#      # Register CSV dialect
#     csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    
#     # Open the output CSV file for writing
#     with open(output_file_name, 'w', encoding='utf8', newline='') as f:
#         writer = csv.writer(f, dialect='myDialect')
        
#         # Write the header row
#         writer.writerow(target_columns)
        
#         # Iterate through rows and write selected columns
#         for row in full_data_rows_list:
#             # Skip empty rows
#             if not any(row):
#                 continue
            
#             # Extract values for the target columns dynamically based on their indexes
#             target_values = [row[index] for index, col_name in enumerate(target_columns)]
#             writer.writerow(target_values)









def categorize_time_of_day(hour):
    if 6 <= hour < 12:
        return 'morning'
    elif 12 <= hour < 18:
        return 'afternoon'
    else:
        return 'evening'



def load_telemetry_data(session, file):
    '''
    Processes data and loads it into Apache Cassandra tables.
    
    '''
    # Read data from csv file
    print('Reading file: {}'.format(file))
    print('-------------------------')
    print('')
    df = pd.read_csv(file)
    print('Finished reading file')
    print('-------------------------')
    print('')

    
    print('Starting conversions....')
    print('-------------------------')
    print('')
    # Convert scientific notation to regular float for timestamps
    df['ts'] = df['ts'].apply(lambda x: float(x))
    # Convert timestamp column to datetime
    df['ts_datetime'] = pd.to_datetime(df['ts'], unit='s')
    # Convert datetime objects to strings in the required format
    df['ts'] = df['ts_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')


    # Device-Location Mapping
    device_location_map = {
        '00:0f:00:70:91:0a': 'location-1',
        '1c:bf:ce:15:ec:4d': 'location-2',
        'b8:27:eb:bf:9d:51': 'location-3'
    }
    # Add a new column 'location' based on the device
    df['location'] = df['device'].map(device_location_map)
    


    # Add a new column 'time_of_day' based on the timestamp
    df['time_of_day'] = df['ts_datetime'].apply(lambda x: categorize_time_of_day(x.hour))
    print('Finished conversions')
    print('-------------------------')
    print('')


    print('Inserting into raw_sensor_data table.....')
    #Insert data into raw_sensor_data table
    for _, row in df.iterrows():
        try:
            session.execute(raw_sensor_data_insert, (row['ts'], row['device'], row['co'], row['humidity'], row['light'], row['lpg'], row['motion'], row['smoke'], row['temp']))
        except Exception as e:
            print(e)
    print('Finished inserting into raw_sensor_data table')
    print('-------------------------')
    print('')


    print('Inserting into location_based_data table.... ')
    # Insert data into location_based_data table
    for _, row in df.iterrows():
        try:
            session.execute(location_based_data_insert, (row['location'], row['ts'], row['co'], row['humidity'], row['light'], row['lpg'], row['motion'], row['smoke'], row['temp']))
        except Exception as e:
            print(e)
    print('Finished Inserting into location_based_data table ')
    print('-------------------------')
    print('')

    
    print('Inserting into time_of_day_based_data table.....')
    #Insert data into time_of_day_based_data table
    for _, row in df.iterrows():
        try:
            session.execute(time_of_day_based_data_insert, (row['time_of_day'], row['ts'], row['location'], row['co'], row['humidity'], row['light'], row['lpg'], row['motion'], row['smoke'], row['temp']))
        except Exception as e:
            print(e)
    print('Finished Inserting into time_of_day_based_data table')
    print('-------------------------')
    print('')

    print('Inserting into data_based_on_location_and_time_of_day_data table.....')
    # Insert data into data_based_on_location_and_time_of_day_data table
    for _, row in df.iterrows():
        try:
            session.execute(data_based_on_location_and_time_of_day_data_insert, (row['location'], row['time_of_day'], row['ts'], row['co'], row['humidity'], row['light'], row['lpg'], row['motion'], row['smoke'], row['temp']))
        except Exception as e:
            print(e)
    print('Finished Inserting into data_based_on_location_and_time_of_day_data table')
    print('-------------------------')
    print('')


    






def run_pipeline(session, folder_name, func):
    '''
    Runs the ETL pipeline.
    '''

    # # get total number of files found
    num_files = len(get_file_path_list(folder_name))
    print('{} files found in {}'.format(num_files, folder_name))

    #iterate over files and process
    for i, datafile in enumerate(get_file_path_list(folder_name), 1):
        func(session, datafile)
        print('{}/{} files processed.'.format(i, num_files))
    




def main():    
    cluster = Cluster(['127.0.0.1'])
    # To establish connection and begin executing queries, need a session
    session = cluster.connect()
    session.set_keyspace('iot')

    # Run the ETL pipeline
    run_pipeline(session, folder_name='telemetry_source_data', func=load_telemetry_data)

    session.shutdown()
    cluster.shutdown()
if __name__ == "__main__":
    main()

