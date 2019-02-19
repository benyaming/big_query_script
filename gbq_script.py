import os
import sys
from csv import writer
from datetime import datetime

from google.cloud import bigquery

import config

DATA_FP = '{}/{}.csv'.format(config.FILE_PATH, config.FILE_NAME)
ERROR_FP = '{}/{}.log'.format(config.ERROR_FILE_PATH, config.ERROR_FILE_NAME)

FILE_HANDLERS = {
    'data': open(DATA_FP, 'ab'),
    'error': open(ERROR_FP, 'ab')
}
DATA_WRITER = {'dw': writer(FILE_HANDLERS['data'], delimiter=',', quotechar='"')}


def init():
    """
    Make some preparations before downloading process
    """
    # register credentials
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.KEY_PATH

    # add utf-8 support (python 2.7 crutch)
    reload(sys)
    sys.setdefaultencoding('utf8')


def get_now():
    return datetime.now().strftime('%Y-%m-%d_%H-%M-%S')


def check_file_size(data, max_file_size, error=False):
    """
    Checks if file already too large for adding extra information
    In that case it calls `rollover` func
    :param max_file_size:
    :param data: data that will be written to file
    :param error: bool. True if data is error message, False if it data row
    """
    current_file_size = os.path.getsize(DATA_FP if not error else ERROR_FP)
    data_size = sys.getsizeof(data)

    if current_file_size + data_size > max_file_size:
        rollover(error=error)


def write_error_to_log(exception, description):
    """
    Writes error message to error file.
    :param exception: exception data
    :param description: exception description
    """
    msg = '{}, {}\n{}\n========================\n'.format(
        get_now(), description, exception)
    check_file_size(msg, config.ERROR_FILE_SIZE * 1024, error=True)
    FILE_HANDLERS['error'].write(msg)


def rollover(error=False):
    """
    Renames current file to 'archive' version with datetime in filename,
    and delete too old files if count of them more then value in config
    :param error: bool. True if error file, False if  data file
    :return:
    """
    file_type = 'data' if not error else 'error'
    file_name = config.FILE_NAME if not error else config.ERROR_FILE_NAME
    file_path = config.FILE_PATH if not error else config.ERROR_FILE_PATH
    file_count = config.FILE_COUNT if not error else config.ERROR_FILE_COUNT
    extension = 'csv' if not error else 'log'

    # close file before editing it
    FILE_HANDLERS[file_type].close()

    # rename current file
    new_filename = '{}-{}.{}'.format(file_name, get_now(), extension)
    os.rename('{}.{}'.format(file_name, extension), new_filename)

    # Check files count and delete old files if need

    # filter data or error files from all files in directory
    files = filter(lambda x: True if file_name in x else False, os.listdir(file_path))
    # sorting files by creation date
    files = sorted(files, key=os.path.getctime)
    if len(files) >= file_count:
        # deleting the oldest
        os.remove('{}/{}'.format(file_path, files[0]))

    # reopening file
    FILE_HANDLERS[file_type] = open(DATA_FP if not error else ERROR_FP, 'ab')
    # recreate csv writer
    if not error:
        DATA_WRITER['dw'] = writer(FILE_HANDLERS['data'], delimiter=',', quotechar='"')


def load_data():
    """
    Main function. Connects to Google BigQuery, executes query, loads data
    and writes it to csv file
    """

    # Check connection
    try:
        client = bigquery.Client()
    except Exception as e:
        write_error_to_log(e, 'Connection error')
        return
    else:
        query_job = client.query(query=config.SQL_QUERY)

    # Check if query is executable
    try:
        result = query_job.result()
    except Exception as e:
        write_error_to_log(e, 'Query executing error')
        return

    # Working with data row-per-row
    for row in result:
        check_file_size(str(row), config.FILE_SIZE * 1024)

        try:
            DATA_WRITER['dw'].writerow(row.values())
        except Exception as e:
            write_error_to_log(e, 'Writing to file error')


def on_finish():
    for f in FILE_HANDLERS.values():
        f.close()


if __name__ == '__main__':
    init()
    load_data()
    on_finish()
