import os
from datetime import datetime

from main import get_root_path


def delete_all_logs(folder):
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(e)


def delete_all_ddl_logs(folder):
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        if 'DDL' in the_file:
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                print(e)


def delete_logs_older_than_today(folder):
    today = datetime.now()
    today = today.replace(hour=00, minute=00, second=00)
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        datetime_string = the_file[11:30]
        datetime_date = datetime.strptime(datetime_string, '%Y-%m-%d %H.%M.%S')
        if datetime_date < today:
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                print(e)


delete_all_logs(get_root_path() + '/logs/')
