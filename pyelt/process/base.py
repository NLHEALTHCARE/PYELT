from typing import Dict, List, Any


class BaseProcess():
    def __init__(self, owner: 'Pipe'):
        import pyelt.pipeline
        if isinstance(owner, pyelt.pipeline.Pipe):
            self.pipe = owner
            self.pipeline = self.pipe.pipeline
            self.logger = self.pipe.logger
        elif isinstance(owner, pyelt.pipeline.Pipeline):
            self.pipeline = owner
            self.logger = self.pipeline.logger
        self.dwh = self.pipeline.dwh
        self.runid = self.pipeline.runid

        self.sql_logger = self.pipeline.sql_logger

    def execute(self, sql: str, log_message: str='') -> None:
        self.sql_logger.log_simple(sql + '\r\n\r\n------------------------------\r\n')
        try:
            rowcount = self.dwh.execute(sql, log_message)
            self.logger.log(log_message, rowcount=rowcount, indent_level=5)
        except Exception as err:
            self.logger.log_error(log_message, sql, err.args[0])
            if 'on_errors' in self.pipeline.config and self.pipeline.config['on_errors'] == 'throw':
                raise Exception(err, sql, log_message)

    def execute_read(self, sql: str, log_message: str='') -> List[List[Any]]:
        self.sql_logger.log_simple(sql + '\r\n')
        result = []
        try:
            result = self.dwh.execute_read(sql, log_message)

            self.logger.log(log_message, indent_level=5)
        except Exception as err:
            self.logger.log_error(log_message, sql, err.args[0])
            # raise Exception(err)
            if 'on_errors' in self.pipeline.config and self.pipeline.config['on_errors'] == 'throw':
                raise Exception(err, sql, log_message)
        finally:
            return result

    def execute_without_commit(self, sql: str, log_message: str=''):
        self.sql_logger.log_simple(sql + '\r\n')

        try:
            self.dwh.execute_without_commit(sql, log_message)

            self.logger.log(log_message, indent_level=5)
        except Exception as err:
            self.logger.log_error(log_message, sql, err.args[0])
            # raise Exception(err)
            if 'on_errors' in self.pipeline.config and self.pipeline.config['on_errors'] == 'throw':
                raise Exception(err, sql, log_message)

    def _get_fixed_params(self) -> Dict[str, Any]:
        params = {}
        params['runid'] = self.runid
        return params


