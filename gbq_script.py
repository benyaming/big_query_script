import os
import sys
from csv import writer
from datetime import datetime

from google.cloud import bigquery

import config


# encoding=utf8
reload(sys)
sys.setdefaultencoding('utf8')


def rollover_logfile():
    # rename current file
    now = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    new_filename = '{}-{}'.format(config.FILE_NAME, now)
    os.rename(config.FILE_NAME, new_filename)
    # todo write file extansion after date

    # check files count and delete old files if need
    # filter log files from all files in directory
    files = filter(lambda x: True if config.FILE_NAME in x else False,
                   os.listdir(config.FILE_PATH))
    files = sorted(files, key=os.path.getctime)

    if len(files) >= config.FILE_COUNT:
        os.remove('{}/{}'.format(config.FILE_PATH, files[0]))


with open(config.FILE_NAME, 'a') as f:
    pass

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.KEY_path

client = bigquery.Client()
query_job = client.query(query=config.SQL_QUERY)

for row in query_job:
    # data = [i.encode('utf-8') if isinstance(i, str) else i for i in row.values()]
    data = row.values()
    # get curent file size
    file_size = os.path.getsize('{}/{}'.format(config.FILE_PATH, config.FILE_NAME))
    data_size = sys.getsizeof(data)
    max_file_size = config.FILE_SIZE * 1024

    # file too big?
    if file_size + data_size > max_file_size:
        rollover_logfile()

    with open(config.FILE_NAME, 'ab', ) as logfile:
        log_writer = writer(logfile, delimiter=',', quotechar='"')
        log_writer.writerow(data)
