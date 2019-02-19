""" Google BigQuery to CSV downloader config file. """

'''
Credentials file path.
Write here absolute path to JSON key file to your Google BigQuery accaunt
The key may be generated here: 
https://console.cloud.google.com/apis/credentials/serviceaccountkey
Example: '/home/data/key.json'
'''
KEY_PATH = 'place_path_here'

# SQL query to execute
SQL_QUERY = 'place_sql_here'

# Data file options
FILE_FILDER = ''  # Absolute path to data files folder. Ex: '/home/data/'
FILE_NAME = ''  # Data file name without extansion. Ex: 'data'
FILE_SIZE = 0  # Data file size in KBytes
FILE_COUNT = 0  # Number of data files to store

# Error file options
ERROR_FILE_FOLDER = ''  # Absolute path to error files folder. Ex: '/home/data/errors/'
ERROR_FILE_NAME = ''  # Error file name without extansion. Ex: 'error'
ERROR_FILE_SIZE = 0  # Error file size in KBytes
ERROR_FILE_COUNT = 0  # Number of error files to store
