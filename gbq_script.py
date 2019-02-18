import os
import sys
from csv import writer
from datetime import datetime

from google.cloud import bigquery

import config

DATA_FP = '{}/{}.csv'.format(config.FILE_PATH, config.FILE_NAME)
ERROR_FP = '{}/{}.log'.format(config.ERROR_FILE_PATH, config.ERROR_FILE_NAME)


def init():
    """
    Make some preparations before downloading process
    """
    # register credentials
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.KEY_PATH

    # add utf-8 support (python 2.7 crutch)
    reload(sys)
    sys.setdefaultencoding('utf8')

    # create data- and log-files (like touch)
    with open(DATA_FP, 'a') as f:
        pass
    with open(ERROR_FP, 'a') as f:
        pass


def get_now():
    return datetime.now().strftime('%Y-%m-%d_%H-%M-%S')


def check_file_size(data, error=False):
    """
    Checks if file already too large for adding extra information
    In that case it calls `rollover` func
    :param data: data that will be written to file
    :param error: bool. True if data is error message, False if it data row
    """
    current_file_size = os.path.getsize(DATA_FP if not error else ERROR_FP)
    max_file_size = config.FILE_SIZE if not error else config.ERROR_FILE_SIZE
    max_file_size *= 1024  # because there are KBytes in config
    data_size = sys.getsizeof(data)

    if current_file_size + data_size > max_file_size:
        rollover(error=error)


def write_error_to_log(msg):
    """
    Writes error message to error file.
    Message format:
        yyyy-mm-dd_hh-mm-ss, error type
        Error message
        ========================
    :param msg: Error type
    """
    check_file_size(msg, error=True)

    with open(ERROR_FP, 'ab') as error_log:
        error_log.write(msg)


def rollover(error=False):
    """
    Renames current file to 'archive' version with datetime in filename,
    and delete too old files if count of them more then value in config
    :param error: bool. True if error file, False if  data file
    :return:
    """
    file_name = config.FILE_NAME if not error else config.ERROR_FILE_NAME
    file_path = config.FILE_PATH if not error else config.ERROR_FILE_PATH
    file_count = config.FILE_COUNT if not error else config.ERROR_FILE_COUNT
    extension = 'csv' if not error else 'log'

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


def load_data():
    """
    Main function. Connects to Google BigQuery, executes query, loads data
    and writes it to csv file
    """

    # Check connection
    try:
        client = bigquery.Client()
    except Exception as e:
        error_message = '{}, {}\n{}\n========================\n'.format(
            get_now(), 'Connection error', e)
        write_error_to_log(error_message)
        exit()
    else:
        query_job = client.query(query=config.SQL_QUERY)

    # Check if query is executable
    try:
        for row in query_job:
            pass
    except Exception as e:
        error_message = '{}, {}\n{}========================\n'.format(
            get_now(), 'Query executing error', e)
        write_error_to_log(error_message)
        exit()

    # Working with data row-per-row
    for row in query_job:
        data = row.values()
        check_file_size(data)

        # Writing row to csv
        with open(DATA_FP, 'ab', ) as logfile:
            log_writer = writer(logfile, delimiter=',', quotechar='"')
            try:
                log_writer.writerow(data)
            except Exception as e:
                error_message = '{}, {}\n{}========================\n'.format(
                    get_now(), 'Writing to file error', e)
                write_error_to_log(error_message)


if __name__ == '__main__':
    init()
    load_data()
