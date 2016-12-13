from typing import List

from pyelt.datalayers.database import DBStatus
from pyelt.datalayers.dv import DvEntity, Sat
from pyelt.datalayers.dwh import Dwh


class Controller:
    entity_cls = DvEntity

    def __init__(self, dwh: 'Dwh'):
        self.dwh = dwh

    def new(self, bk='', runid=0) -> DvEntity:
        entity = self.entity_cls()
        entity.__dict__['dbstatus'] = DBStatus.new
        entity.__dict__['bk'] = bk
        entity.__dict__['_runid'] = runid
        entity.__dict__['type'] = self.entity_cls.cls_get_name()
        for sat_name, sat_cls in self.__class__.entity_cls.cls_get_sats().items():
            sat = sat_cls()
            sat.__dict__['_id'] = 0
            sat.__dict__['dbstatus'] = DBStatus.initialised
            entity.__dict__[sat_name.lower()] = sat
            setattr(sat_cls, 'load', self.load_sat)
        return entity

    def load_old(self, bk='', filter='') -> List[DvEntity]:
        "Load by view"
        return_list = []
        if bk:
            filter = "bk = ''".format(bk)
        if not filter:
            filter = '1 = 1'
        view_name = self.__class__.entity_cls.cls_get_view_name()
        params = {'dv': 'dv', 'view': view_name, 'filter': filter}
        sql = """SELECT * FROM {dv}.{view} WHERE {filter}""".format(**params)
        rows = self.dwh.execute_read(sql, '')
        for row in rows:
            entity = self.__class__.entity_cls()
            for field_name, field_value in row.items():
                entity.__dict__[field_name] = field_value
                entity.__dict__['dbstatus'] = DBStatus.unchanged
                entity.__dict__['loaded_row'] = row
                for sat_name, sat_cls in self.__class__.entity_cls.cls_get_sats().items():
                    sat = sat_cls()
                    sat.__dict__['dbstatus'] = DBStatus.unchanged
                    sat.__dict__['_id'] = row['_id']
                    entity.__dict__[sat_name] = sat
                    for view_field_name, field_value in row.items():
                        field_name = view_field_name.replace(sat_name.lower() + '_', '')
                        if field_name in sat_cls.__dict__:
                            sat.__dict__[field_name] = field_value

            return_list.append(entity)
        return return_list

    def load_by_hub(self, bk='', filter='') -> List[DvEntity]:
        return_list = []
        if bk:
            filter = "bk = ''".format(bk)
        if not filter:
            filter = '1 = 1'
        hub = self.__class__.entity_cls.cls_get_hub_name()
        params = {'dv': 'dv', 'hub': hub, 'filter': filter}
        sql = """SELECT * FROM {dv}.{hub} WHERE {filter}""".format(**params)
        rows = self.dwh.execute_read(sql, '')
        for row in rows:
            entity = self.__class__.entity_cls()
            entity.__dict__['dbstatus'] = DBStatus.initialised
            for field_name, field_value in row.items():
                entity.__dict__[field_name] = field_value
            for sat_name, sat_cls in self.__class__.entity_cls.cls_get_sats().items():
                sat = sat_cls()
                sat.__dict__['_id'] = row['_id']
                sat.__dict__['dbstatus'] = DBStatus.initialised
                # uitzoeken hoe parameter mee te geven aan functie
                entity.__dict__[sat_name.lower()] = sat
                setattr(sat_cls, 'load', self.load_sat)

            return_list.append(entity)
        return return_list

    def load_sat(self, sat):
        print(sat.cls_get_name())
        params = {'dv': 'dv', 'sat': sat.__class__.cls_get_name(), 'id': sat._id}
        sql = """SELECT * FROM {dv}.{sat} WHERE _id = {id} AND _active""".format(**params)
        print(sql)
        rows = self.dwh.execute_read(sql, '')
        if rows:
            row = rows[0]  # {'voornaam': 'asd', 'achternaam': 'asdahgd'}
            for field_name, field_value in row.items():
                sat.__dict__[field_name] = field_value
            sat.__dict__['dbstatus'] = DBStatus.loaded
        else:
            sat.__dict__['dbstatus'] = DBStatus.new

    def save(self, value: List[DvEntity]):
        if isinstance(value, list):
            self.save_entities(value)
        else:
            self.save_entity(value)

    def save_entities(self, value):
        pass

    def save_entity(self, entity):
        # entity = self.check_changed(entity)
        if entity.dbstatus == DBStatus.new:
            self.save_new_hub(entity)
        for sat_name, sat in entity.sats().items():
            if sat.dbstatus == DBStatus.new:
                self.save_new_sat(entity, sat)
            elif sat.dbstatus == DBStatus.changed:
                self.save_sat(sat)

    def save_entity_old(self, entity):
        # entity = self.check_changed(entity)
        if entity.dbstatus == DBStatus.new:
            pass
            # insert hub
            # insert sats
        elif entity.dbstatus == DBStatus.changed:
            for sat_name, sat in entity.sats().items():
                if sat.dbstatus == DBStatus.changed:
                    self.save_sat(sat, 99.02)

    def check_changed_old(self, entity):
        if entity.dbstatus == DBStatus.new:
            return entity
        for sat_name, sat in entity.sats().items():
            for fld_name, fld in sat.__dict__.items():
                view_fld_name = sat_name.lower() + '_' + fld_name
                if view_fld_name in entity.__dict__ and fld != entity.__dict__[view_fld_name]:
                    sat.dbstatus = DBStatus.changed
                    entity.dbstatus = DBStatus.changed
        return entity

    def save_new_hub(self, entity, source_system='sys'):
        params = {}
        params['dv'] = self.dwh.dv.name
        params['hub'] = entity.__class__.cls_get_hub_name()
        params['_runid'] = entity._runid
        params['_source_system'] = source_system
        params['type'] = entity.type
        params['bk'] = entity.bk
        params['fixed_field_names'] = "_runid, _source_system, _insert_date, _valid, _validation_msg, type, bk"
        params['fixed_values'] = "{_runid}, '{_source_system}', now(), True, '', '{type}', '{bk}'".format(**params)
        sql = """INSERT INTO {dv}.{hub} ({fixed_field_names}) SELECT {fixed_values}
WHERE NOT EXISTS (SELECT 1 FROM {dv}.{hub} WHERE bk='{bk}') RETURNING _id;""".format(**params)
        print(sql)
        result = self.dwh.execute_returning(sql, 'INSERT HUB')
        id = result[0][0]
        entity._id = id

    def save_sat(self, sat, source_system='sys'):
        params = {}
        params['dv'] = self.dwh.dv.name
        params['sat'] = sat.__class__.cls_get_name()
        field_names = ''
        field_values = ''
        for fld_name, fld_value in sat.__dict__.items():
            if not (fld_name.startswith('_') or fld_name == 'dbstatus'):
                field_names += fld_name + ', '
                field_values += "'{}', ".format(fld_value)
        field_names = field_names[:-2]
        field_values = field_values[:-2]
        params['fixed_field_names'] = "_id, _runid, _active, _source_system, _insert_date, _finish_date, _revision, _valid, _validation_msg, _hash"
        params['_id'] = sat._id
        params['_runid'] = sat._runid
        params['_source_system'] = source_system
        params['_revision'] = sat._revision
        params['fixed_values'] = "{_id}, {_runid} + 0.01, True, '{_source_system}', now(), NULL, {_revision} + 1, True, '', ''".format(**params)
        params['field_names'] = field_names
        params['values'] = field_values
        params['type'] = ''
        sql = """UPDATE {dv}.{sat} SET _active = False, _finish_date = Now() WHERE _id = {_id} AND _runid = {_runid};

INSERT INTO {dv}.{sat} ({fixed_field_names}, {field_names}) VALUES ({fixed_values}, {values});""".format(**params)
        self.dwh.execute(sql, 'INSERT SAT')
        # sql = """UPDATE {dv}.{sat} previous SET _active = FALSE, _finish_date = current._insert_date
        #                 FROM {dv}.{sat} current WHERE previous._active = TRUE AND previous._id = current._id AND current._revision = (previous._revision + 1);""".format(
        #     **params)
        # self.dwh.execute(sql, '  update sat set old ones inactive')

    def save_new_sat(self, hub, sat, source_system='sys'):
        params = {}
        params['dv'] = self.dwh.dv.name
        params['sat'] = sat.__class__.cls_get_name()
        params['_id'] = hub._id
        params['_runid'] = hub._runid
        params['_source_system'] = source_system
        params['_revision'] = 1
        field_names = ''
        field_values = ''
        for fld_name, fld_value in sat.__dict__.items():
            if not (fld_name.startswith('_') or fld_name == 'dbstatus'):
                field_names += fld_name + ', '
                field_values += "'{}', ".format(fld_value)
        field_names = field_names[:-2]
        field_values = field_values[:-2]
        params['fixed_field_names'] = "_id, _runid, _active, _source_system, _insert_date, _finish_date, _revision, _valid, _validation_msg, _hash"
        params['fixed_values'] = "{_id}, {_runid}, True, '{_source_system}', now(), NULL, {_revision}, True, '', ''".format(**params)
        params['field_names'] = field_names
        params['values'] = field_values
        params['type'] = ''
        sql = """INSERT INTO {dv}.{sat} ({fixed_field_names}, {field_names}) VALUES ({fixed_values}, {values});""".format(**params)
        self.dwh.execute(sql, 'INSERT SAT')
