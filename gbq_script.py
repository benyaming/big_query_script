import os
import sys
from csv import writer
from datetime import datetime
from traceback import format_exc

from google.cloud import bigquery

import config


FILE_HANDLERS = {}
HEADERS = {}


def on_start():
    """
    Make some preparations before downloading process
    """
    # register credentials
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.KEY_PATH

    # add utf-8 support (python 2.7 crutch)
    reload(sys)
    sys.setdefaultencoding('utf8')

    # initialise file descryptors
    FILE_HANDLERS['data'] = open('{}/{}.csv'.format(
        config.FILE_FOLDER, config.FILE_NAME), 'ab')
    FILE_HANDLERS['error'] = open('{}/{}.log'.format(
        config.ERROR_FILE_FOLDER, config.ERROR_FILE_NAME), 'ab')
    FILE_HANDLERS['dw'] = writer(FILE_HANDLERS['data'], delimiter=',', quotechar='"')

    HEADERS['writed'] = True if file_exists(FILE_HANDLERS['data']) else False


def get_now():
    return datetime.now().strftime('%Y-%m-%d_%H-%M-%S')


def file_exists(f):
    """
    Return True if file size more then 0
    :param f: File descriptor
    """
    f.seek(0, 2)
    return f.tell() > 0


def check_file_size(data, current_file_size, max_file_size):
    """
    Checks file size. Returns True if there is no more place
    """
    data_size = sys.getsizeof(data)

    if current_file_size + data_size > max_file_size:
        return True


def rollover(f, file_count):
    """
    Renames current file to 'archive' version with datetime in filename,
    and deletes too old files if count of them more then value in config
    :param file_count:
    :param f:
    :return:
    """
    file_path = f.name
    file_name, extension = file_path.split('/')[-1].split('.')

    # close file before editing it
    f.close()

    # rename current file
    new_filename = '{}-{}.{}'.format(file_name, get_now(), extension)
    os.rename('{}.{}'.format(file_name, extension), new_filename)

    # Check files count and delete old files if need

    # filter data or error files from all files in directory
    folder_path = file_path.split(file_name)[0]
    files = filter(lambda x: True if file_name in x else False, os.listdir(folder_path))
    # sorting files by creation date
    files = sorted(files, key=os.path.getctime)
    if len(files) >= file_count:
        # deleting the oldest
        os.remove('{}/{}'.format(folder_path, files[0]))

    return open(file_path, 'ab')


def write_error(exception):
    """
    Writes error message to error file.
    :param exception: exception data
    """
    msg = '{}\n{}======================================\n'.format(
        get_now(), exception)
    current_file_size = os.path.getsize(FILE_HANDLERS['error'].name)
    file_is_full = check_file_size(msg, current_file_size, config.ERROR_FILE_SIZE * 1024)
    if file_is_full:
        fc = config.ERROR_FILE_COUNT
        FILE_HANDLERS['error'] = rollover(FILE_HANDLERS['error'], fc)
    FILE_HANDLERS['error'].write(msg)


def write_data(headers, data):
    current_file_size = os.path.getsize(FILE_HANDLERS['data'].name)
    file_is_full = check_file_size(data, current_file_size, config.FILE_SIZE * 1024)
    if file_is_full:
        fc = config.FILE_COUNT
        FILE_HANDLERS['data'] = rollover(FILE_HANDLERS['data'], fc)
        FILE_HANDLERS['dw'] = writer(FILE_HANDLERS['data'], delimiter=',', quotechar='"')
        HEADERS['writed'] = False

    # Write csv headers if they haven't been written
    if not HEADERS['writed']:
        FILE_HANDLERS['dw'].writerow(headers)
        HEADERS['writed'] = True

    FILE_HANDLERS['dw'].writerow(data)
    ########
    FILE_HANDLERS['data'].flush()


def load_data():
    """
    Main function. Connects to Google BigQuery, executes query, loads data
    and writes it to csv file
    """

    client = bigquery.Client()
    result = client.query(query=config.SQL_QUERY).result()
    headers = [i.name for i in result.schema]

    # Working with data row-per-row
    for row in result:
        data = row.values()
        write_data(headers=headers, data=data)


def on_finish():
    FILE_HANDLERS['error'].close()
    FILE_HANDLERS['data'].close()


if __name__ == '__main__':
    try:
        on_start()
        load_data()
    except Exception as e:
        write_error(format_exc())
        raise
    finally:
        on_finish()
