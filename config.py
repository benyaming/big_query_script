from os import getcwd


# credentials
KEY_path = '{}/{}'.format(getcwd(), 'key.json')

# SQL query to execute
SQL_QUERY = 'SELECT * FROM temp.stories_copy LIMIT 10 offset 30'

# downloaded file options
FILE_PATH = '{}'.format(getcwd())
FILE_NAME = 'data.csv'
FILE_SIZE = 5  # int, KByte
FILE_COUNT = 3

# error_log file options
ERROR_FILE_PATH = ''
ERROR_FILE_NAME = ''
ERROR_FILE_SIZE = ''
ERROR_FILE_COUNT = ''
