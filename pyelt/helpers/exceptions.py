class PyeltException(Exception):
    def __init__(self, err, err_msg = ''):
        super().__init__(err)
