import datetime

from pyelt.datalayers.database import Schema, Table


class Sys(Schema):
    pass
    # def __init__(self) -> None:
    #     self.runs = Table('runs')
    #
    # def insert_new_run(self) -> None:
    #     #get last run
    #
    #     run = None
    #     if run.date == datetime.datetime.now().date():
    #         run.runid += 0.01
    #     else:
    #         run.runid = int(run.runid) + 1
