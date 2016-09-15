from pyelt.datalayers.database import Column

from pyelt.datalayers.dwh import Dwh


class DbSession():
    _instance = None

    def __new__(cls, *args, **kwargs):
        """ returns cls._instance
        """
        if not cls._instance and (args or kwargs):
            # singleton implementatie
            cls._instance = super(DbSession, cls).__new__(
                cls)
            if args:
                config = args[0]
                runid = args[1]
            else:
                config = kwargs['config']
                runid = kwargs['runid']

            cls._instance.config = config  #: pyelt_config
            cls._instance.dwh = Dwh(config)
            cls._instance.runid = runid  # type: float
        return cls._instance


class DBStatus:
    pre_initialised = 'PREINIT'
    initialised = 'INIT'
    loaded = 'LOADED'
    new = 'NEW'
    changed = 'CHANGED'


class Row():
    def __getattribute__(self, item):
        val = super().__getattribute__(item)
        if isinstance(val, Column):
            val = val.default_value
        return val


class EntityRow():
    def __init__(self, entity):
        self._id = 0
        self.entity = entity

    def __getattribute__(self, item):
        val = super().__getattribute__(item)
        if isinstance(val, SatData):
            if val.db_status == DBStatus.pre_initialised:
                sat_rows = val.load()
                self.entity.sats[item] = val
            else:
                sat_rows = self.entity.sats[item].rows
            if self._id in sat_rows:
                val = sat_rows[self._id]
                # else:
                #     val = val.default_value
        return val


class DvData():
    def __init__(self):
        self.db_status = DBStatus.pre_initialised
        self.rows = {}  # type: Dict[int, Row]
        self.session = DbSession()
        if self.session:
            self.dwh = self.session.dwh
            self.runid = self.session.runid

    def load(self, filter='1=1'):
        pass

    def new(self):
        row = Row()
        row.__dict__['db_status'] = DBStatus.new
        id = len(self.rows) * -1
        row.__dict__['_id'] = id
        row.__dict__['_runid'] = self.runid
        row.__dict__['_source_system'] = 'manual insert'
        # self.rows.append(row)
        self.rows[id] = row
        return row

    def save(self):
        for row in self.rows.values():
            if row.db_status == DBStatus.new:
                self._save_new_row(row)
            elif row.db_status == DBStatus.changed:
                self._update_row(row)

    def _save_new_row(self, row):
        pass

    def _update_row(self, row):
        pass


class HubData(DvData):
    def __init__(self):
        super().__init__()
        self.type = ''

    def load(self, filter='1=1', sats=[]):
        hub_name = self.name
        params = {'dv': 'dv', 'hub': hub_name, 'filter': filter}
        sql = """SELECT * FROM {dv}.{hub} WHERE {filter}""".format(**params)
        rows = self.dwh.execute_read(sql, '')
        for row in rows:
            hub = Row()
            hub.__dict__['_id'] = row['_id']
            hub.__dict__['_runid'] = row['_runid']
            hub.__dict__['bk'] = row['bk']
            hub.__dict__['db_status'] = DBStatus.loaded
            # for sat_cls in sats:
            #     sat = sat_cls()
            #     sat._id = row['_id']
            #     hub.__dict__[sat.name] = sat

            # self.rows.append(hub)
            self.rows[hub._id] = hub

            # entity = self.__class__.entity_cls()
            # entity.__dict__['db_status'] = DBStatus.initialised
            # for field_name, field_value in row.items():
            #     entity.__dict__[field_name] = field_value
            # for sat_name, sat_cls in self.__class__.entity_cls.get_sats().items():
            #     sat = sat_cls()
            #     sat.__dict__['_id'] = row['_id']
            #     sat.__dict__['dbstatus'] = DBStatus.initialised
            #     # uitzoeken hoe parameter mee te geven aan functie
            #     entity.__dict__[sat_name.lower()] = sat
            #     setattr(sat_cls, 'load', self.load_sat)

            # return_list.append(entity)
        return self.rows

    def new(self):
        row = super().new()
        row.__dict__['type'] = self.type
        row.__dict__['bk'] = ''
        return row

    def _save_new_row(self, row):
        params = {}
        params['dv'] = self.dwh.dv.name
        params['hub'] = self.name
        params['_runid'] = row._runid
        params['_source_system'] = row._source_system
        params['type'] = row.type
        params['bk'] = row.bk
        params['fixed_field_names'] = "_runid, _source_system, _insert_date, _valid, _validation_msg, type, bk"
        params['fixed_values'] = "{_runid}, '{_source_system}', now(), True, '', '{type}', '{bk}'".format(**params)
        sql = """INSERT INTO {dv}.{hub} ({fixed_field_names}) SELECT {fixed_values}
WHERE NOT EXISTS (SELECT 1 FROM {dv}.{hub} WHERE bk='{bk}') RETURNING _id;""".format(**params)
        print(sql)
        result = self.dwh.execute_returning(sql, 'INSERT HUB')
        row.db_status = DBStatus.loaded
        if result:
            id = result[0][0]
            row._id = id
        else:
            # bestaat al in db: id laden
            sql = """SELECT _id FROM {dv}.{hub} WHERE bk='{bk}';""".format(**params)
            result = self.dwh.execute_returning(sql, 'GET HUB ID BY BK')
            id = result[0][0]
            row._id = id
        print(sql)
        result = self.dwh.execute_returning(sql, 'INSERT HUB')

        return row


class SatData(DvData):
    def __init__(self):
        super().__init__()
        self.entity = None

    # def __getattribute__(self, item):
    #     val = super().__getattribute__(item)
    #     if isinstance(val, Column):
    #         if self.db_status == DBStatus.pre_initialised:
    #             rows = self.load()
    #     return val

    def load(self, filter='1=1'):
        sat_name = self.name
        params = {'dv': 'dv', 'sat': sat_name, 'filter': filter}
        sql = """SELECT * FROM {dv}.{sat} WHERE {filter} AND _active""".format(**params)
        print(sql)
        rows = self.dwh.execute_read(sql, '')
        for row in rows:
            sat = Row()
            sat.__dict__['db_status'] = DBStatus.loaded
            for field_name, field_value in row.items():
                sat.__dict__[field_name] = field_value
            # self.rows.append(sat)
            self.rows[sat._id] = sat
        self.db_status = DBStatus.loaded
        return self.rows

    def new(self):
        row = super().new()
        row.__dict__['_revision'] = 0
        cols = self.__class__.get_columns()
        for col in cols:
            row.__dict__[col.name] = col.default_value
        return row

    def _save_new_row(self, row):
        params = {}
        params['dv'] = self.dwh.dv.name
        params['sat'] = self.name
        params['_id'] = row._id
        params['_runid'] = row._runid
        params['_source_system'] = row._source_system
        params['_revision'] = 1
        field_names = ''
        field_values = ''
        for fld_name, fld_value in row.__dict__.items():
            if not (fld_name.startswith('_') or fld_name == 'db_status'):
                field_names += fld_name + ', '
                field_values += "'{}', ".format(fld_value)
        field_names = field_names[:-2]
        field_values = field_values[:-2]
        params['fixed_field_names'] = "_id, _runid, _active, _source_system, _insert_date, _finish_date, _revision, _valid, _validation_msg, _hash"
        params['fixed_values'] = "{_id}, {_runid}, True, '{_source_system}', now(), NULL, {_revision}, True, '', ''".format(**params)
        params['field_names'] = field_names
        params['values'] = field_values
        params['type'] = ''
        sql = """INSERT INTO {dv}.{sat} ({fixed_field_names}, {field_names}) SELECT {fixed_values}, {values}
WHERE NOT EXISTS (SELECT 1 FROM {dv}.{sat} WHERE _id={_id} AND _runid = {_runid}) RETURNING _id""".format(**params)
        result = self.dwh.execute_returning(sql, 'INSERT SAT')
        row.db_status = DBStatus.loaded
        if not result:
            # bestaat al in db: _revision updaten
            old_row = self._load_row(params['_id'])
            self._update_row(old_row, row)
        return row

    def _load_row(self, id):
        sat_name = self.name
        filter = '_id = {} AND _active'.format(id)
        params = {'dv': 'dv', 'sat': sat_name, 'filter': filter}
        sql = """SELECT * FROM {dv}.{sat} WHERE {filter}""".format(**params)
        print(sql)
        rows = self.dwh.execute_read(sql, '')
        if rows:
            row = rows[0]
            sat = Row()
            sat.__dict__['db_status'] = DBStatus.loaded
            for field_name, field_value in row.items():
                sat.__dict__[field_name] = field_value
            return sat

    def _update_row(self, old_row, new_row):
        is_changed = False
        for fld_name, fld_value in new_row.__dict__.items():
            if not (fld_name.startswith('_') or fld_name == 'db_status'):
                if fld_value != old_row.__dict__[fld_name]:
                    is_changed = True
                    break
        if not is_changed:
            return

        params = {}
        params['dv'] = self.dwh.dv.name
        params['sat'] = self.name
        params['_id'] = new_row._id
        params['_previous_runid'] = old_row._runid
        new_row._runid = float(old_row._runid) + 0.01
        params['_runid'] = new_row._runid
        params['_source_system'] = new_row._source_system
        new_row._revision = old_row._revision + 1
        params['_revision'] = new_row._revision
        field_names = ''
        field_values = ''
        for fld_name, fld_value in new_row.__dict__.items():
            if not (fld_name.startswith('_') or fld_name == 'db_status'):
                field_names += fld_name + ', '
                field_values += "'{}', ".format(fld_value)
        field_names = field_names[:-2]
        field_values = field_values[:-2]
        params['fixed_field_names'] = "_id, _runid, _active, _source_system, _insert_date, _finish_date, _revision, _valid, _validation_msg, _hash"
        params['fixed_values'] = "{_id}, {_runid}, True, '{_source_system}', now(), NULL, {_revision}, True, '', ''".format(**params)
        params['field_names'] = field_names
        params['values'] = field_values
        params['type'] = ''

        sql = """UPDATE {dv}.{sat} SET _active = False, _finish_date = Now() WHERE _id = {_id} AND _runid = {_previous_runid};

INSERT INTO {dv}.{sat} ({fixed_field_names}, {field_names}) VALUES ({fixed_values}, {values});""".format(**params)
        self.dwh.execute(sql, 'INSERT SAT')
        return new_row

        # def __getattribute__(self, item):
        #     if item.startswith('__') or item == 'dbstatus' or item == 'load' or item == 'get_name' or item == '_id':
        #         return super().__getattribute__(item)
        #     else:
        #         if self.dbstatus == DBStatus.initialised and \
        #                         self.dbstatus != DBStatus.loaded and \
        #                 self.load:
        #             self.load(self)
        #
        #             return super().__getattribute__(item)
        #
        # def __setattr__(self, key, value):
        #     if self.__dict__['db_status'] == DBStatus.initialised and \
        #                     self.__dict__['db_status'] != DBStatus.loaded and \
        #             self.load:
        #         self.load(self)
        #     if key in self.__dict__ and self.__dict__[key] != value:
        #         self.__dict__['db_status'] = DBStatus.changed
        #         print(self.__dict__[key], '=>', value)
        #     super().__setattr__(key, value)


class EntityData(DvData):
    def __init__(self):
        super().__init__()
        self.hub = self.get_hub()
        sats_cls = self.__class__.get_sats()
        self.sats = {}
        for sat_name, sat_cls in sats_cls.items():
            sat = sat_cls()
            self.sats[sat_name.lower()] = sat

    def load(self, filter='1=1', sats=[]):

        hub_name = self.hub.name
        params = {'dv': 'dv', 'hub': hub_name, 'filter': filter}
        sql = """SELECT * FROM {dv}.{hub} WHERE {filter}""".format(**params)
        rows = self.dwh.execute_read(sql, '')
        for row in rows:
            entity = EntityRow(self)
            entity.__dict__['_id'] = row['_id']
            entity.__dict__['_runid'] = row['_runid']
            entity.__dict__['bk'] = row['bk']
            entity.__dict__['db_status'] = DBStatus.loaded
            for sat_name, sat in self.sats.items():
                entity.__dict__[sat_name] = sat

            # self.rows.append(hub)
            self.rows[entity._id] = entity

            # entity = self.__class__.entity_cls()
            # entity.__dict__['db_status'] = DBStatus.initialised
            # for field_name, field_value in row.items():
            #     entity.__dict__[field_name] = field_value
            # for sat_name, sat_cls in self.__class__.entity_cls.get_sats().items():
            #     sat = sat_cls()
            #     sat.__dict__['_id'] = row['_id']
            #     sat.__dict__['dbstatus'] = DBStatus.initialised
            #     # uitzoeken hoe parameter mee te geven aan functie
            #     entity.__dict__[sat_name.lower()] = sat
            #     setattr(sat_cls, 'load', self.load_sat)

            # return_list.append(entity)
        return self.rows

        # def __getattribute__(self, item):
        #     val = super().__getattribute__(item)
        #     if isinstance(val, SatData):
        #         if val.db_status == DBStatus.pre_initialised:
        #             val._load_row(self._id)
        #     return val
