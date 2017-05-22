import logging
from datetime import datetime
from time import sleep

# import pipelines.clinics.clinics_configs as pyelt_configs

# done: sql, ddl en main in eigen folder
# done: folders auto aanmaken
# done: naar console met kleuren, naar file zonder kleur (of als rtf?)
# done: foutmeldingen beter integreren in log
# done: betere inspringing en ordening
# done: bij fout -> bestand hernoemen zodat je kunt zien dat er iets fout ging
# done: consolehelper integreren
# done: op print zoeken

__author__ = 'hvreenen'
class LoggerTypes:
    MAIN = 'MAIN'
    SQL = 'SQL'
    DDL = 'DDL'
    ERROR = 'ERROR'
    PULL_DATA = 'PULL_DATA'


class ConsoleColors:
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    GRAY = '\033[91m'
    LIGHTGRAY = '\033[37m'
    RED = '\033[91m'
    FILLED_RED = '\033[101m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

class Logger:
    def __init__(self):
        self.logger = None #type: logging.logger
        self.start_time = datetime.now()  # type: datetime.datetime
        self.last_start_time = datetime.now()  # type: datetime.datetime
        self.errors = [] #type: List[str]
        self.to_console = True  # type: bool
        self.filename = ''  # type: str

    @staticmethod
    def create_logger(logger_type: str = LoggerTypes.MAIN, runid: float = 0, configs={}, to_console: bool = True, filename_args = '') -> 'Logger':
        logger = logging.getLogger(logger_type)
        # create file
        path, filename = Logger.__create_path_and_filename_by_type(logger_type, runid, configs, filename_args)
        if len(logger.handlers) == 0:
            # create formatter
            if logger_type == LoggerTypes.MAIN:
                formatter = logging.Formatter('%(asctime)s - %(message)s')
            else:
                formatter = logging.Formatter('%(message)s')

            fileHandler = logging.FileHandler(path + filename)
            fileHandler.setFormatter(formatter)
            logger.addHandler(fileHandler)
            # if to_console:
            #     consoleHandler = logging.StreamHandler()
            #     consoleHandler.setFormatter(formatter)
            #     logger.addHandler(consoleHandler)
            logger.setLevel(logging.INFO)
            # logger.errors = []
        log_obj = Logger()
        log_obj.logger = logger
        log_obj.start_time = datetime.now()
        log_obj.last_start_time = datetime.now()
        log_obj.errors = []  # logger.errors
        log_obj.to_console = to_console
        if 'log_to_console' in configs:
            log_obj.to_console = configs['log_to_console']
        log_obj.filename = filename
        log_obj.config = configs
        return log_obj

    @staticmethod
    def __create_path_and_filename_by_type(logger_type: str = LoggerTypes.MAIN, runid=0.00, configs={}, filename_args = ''):
        import os
        from main import get_root_path
        path = get_root_path() + configs['log_path']
        if logger_type == LoggerTypes.SQL and 'sql_log_path' in configs:
            path = get_root_path() + configs['sql_log_path']
        if logger_type == LoggerTypes.DDL and 'ddl_log_path' in configs:
            path = get_root_path() + configs['ddl_log_path']
        if not os.path.exists(path):
            os.makedirs(path)
        filename = 'LOG {0:%Y-%m-%d %H.%M.%S} {1}.log'.format(datetime.now(), logger_type)
        if runid:
            filename = 'LOG {1:%Y-%m-%d %H.%M.%S} RUN{0:07.2f} {2}.log'.format(runid, datetime.now(), logger_type)
        if filename_args:
            filename = filename.replace('.log', '_' + filename_args + '.log')

        return path, filename

    def log_simple(self, msg: str) -> None:
        self.logger.info(self.strip_formatting_tags(msg))
        if self.to_console:
            Logger.pprint(msg)

    def log(self, descr: str, last_start_time: datetime = None, rowcount: int = -1, newline: bool = False, indent_level=0) -> None:
        if not last_start_time:
            last_start_time = self.last_start_time
        newline_str = '' #type: str
        if newline:
            newline_str = '\r\n'

        end_time = datetime.now()
        global_time_descr = ''
        if last_start_time:
            global_elapsed_time = end_time - self.start_time
            elapsed_time_since_last_log = end_time - last_start_time
            global_time_descr = self.time_descr(global_elapsed_time)

        if descr:
            if indent_level >= 4:
                descr = '-' + descr
            for i in range(indent_level):
                descr = ' ' + descr
            for i in range(len(self.strip_formatting_tags(descr)), 60):
                # uitvullen tot 50 positities
                descr += ' '
                # divmod(elapsedTime.total_seconds(), 60)
        if descr and self.logger:
            if global_time_descr:
                if rowcount >= 0:
                    descr = '{0} <lightgray>executed on {1} rows in {2} ({3} since start) {4}</>'.format(descr, rowcount, self.time_descr(elapsed_time_since_last_log), global_time_descr, newline_str)
                else:
                    descr = '{0} <lightgray>executed in {1} ({2} since start) {3}</>'.format(descr, self.time_descr(elapsed_time_since_last_log), global_time_descr, newline_str)
            else:
                descr = '{}{}'.format(descr, newline_str)
        self.logger.info(self.strip_formatting_tags(descr))
        if self.to_console:
            Logger.pprint(descr)
        self.last_start_time = end_time

    def log_error(self, log_msg, sql='', err_msg= '', ex = None):
        if ex:
            for arg in ex.args:
                err_msg += str(arg)
        while err_msg.endswith('\n'):
            err_msg = err_msg[:-1]
        msg = """
<red>==========================================================================
ERROR tijdens : {}

SQL: {}

ERROR: {}
==========================================================================</>""".format(log_msg, sql, err_msg)
        if 'on_errors' in self.config and self.config['on_errors'] == 'throw':
            raise Exception(msg)
        else:

            self.logger.error(self.strip_formatting_tags(msg))
            self.errors.append(msg)
            if self.to_console:
                Logger.pprint(msg)
                # even wachten opdat de log-msg niet verward raakt door de foutmelding
                sleep(0.1)


    def time_descr(self, elapsed_time) -> str:
        sec = elapsed_time.total_seconds()
        time_descr = '{0:.3f} seconds'.format(sec)

        if sec > 60:
            import math
            min = math.floor(sec / 60)
            rest_sec = sec - min * 60
            time_descr = '{0} minutes and {1:.3f} seconds'.format(min, rest_sec)
        return time_descr

    @staticmethod
    def pprint(value):
        """Maakt het mogelijk in kleur te printen in de console
        Gebruik html-achtige tags zoals: <red>rood</>
        """
        if isinstance(value, list):
            for val in value:
                Logger.pprint(val)
        elif isinstance(value, str):
            value = value.replace('<br>', '\r\n')
            value = value.replace('<\br>', '\r\n')
            value = value.replace('<b>', ConsoleColors.BOLD)
            value = value.replace('<u>', ConsoleColors.UNDERLINE)
            value = value.replace('<red>', ConsoleColors.RED)
            value = value.replace('<gray>', ConsoleColors.GRAY)
            value = value.replace('<lightgray>', ConsoleColors.LIGHTGRAY)
            value = value.replace('<yellow>', ConsoleColors.YELLOW)
            value = value.replace('<blue>', ConsoleColors.BLUE)
            value = value.replace('<green>', ConsoleColors.GREEN)
            value = value.replace('<purle>', ConsoleColors.PURPLE)
            value = value.replace('<cyan>', ConsoleColors.CYAN)
            value = value.replace('<darkcyan>', ConsoleColors.DARKCYAN)
            value = value.replace('<filledred>', ConsoleColors.FILLED_RED)
            value = value.replace('</>', ConsoleColors.END)
            print(value)
        else:
            print(value)

    def strip_formatting_tags(self, value):
        value = value.replace('<br>', '\r\n')
        value = value.replace('<\br>', '\r\n')
        value = value.replace('<b>', '')
        value = value.replace('<u>', '')
        value = value.replace('<red>', '')
        value = value.replace('<gray>', '')
        value = value.replace('<lightgray>', '')
        value = value.replace('<yellow>', '')
        value = value.replace('<blue>', '')
        value = value.replace('<green>', '')
        value = value.replace('<purle>', '')
        value = value.replace('<cyan>', '')
        value = value.replace('<darkcyan>', '')
        value = value.replace('<filledred>', '')
        value = value.replace('</>', '')
        value = value.replace('\ufeff', '')
        return value

    def test_show_all_colors(self):
        for i in range(120):
            s = '\033[{}mDIT IS FORMAT. \033[0mDit niet.'.format(i)
            print(i, s)
