from pyelt.datalayers.database import Table, Schema
from pyelt.datalayers.dv import HybridSat
from pyelt.datalayers.sor import SorTable, SorQuery
from pyelt.mappings.base import ConstantValue
from pyelt.mappings.sor_to_dv_mappings import SorToEntityMapping, SorToLinkMapping, SorToValueSetMapping
from pyelt.mappings.validations import DvValidation, SorValidation
from pyelt.sources.databases import SourceTable, SourceQuery
from pyelt.sources.files import File, CsvFile
from pyelt.helpers.pyelt_logging import Logger, LoggerTypes
from pyelt.process.base import BaseProcess


class BaseEtl(BaseProcess):
    def __init__(self, pipe):
        super().__init__(pipe)

    def copy_to_exceptions_table(self, from_table, schema=None):
        params = self._get_fixed_params()
        if isinstance(from_table, str):
            from_table = Table(from_table, schema)

        params['from_table'] = from_table.name
        params['schema'] = from_table.schema.name

        from_table.reflect()
        key_values = ''
        for key in from_table.key_names:
            key_values += "'{0}: ' || {0} || ',' ||".format(key)
        key_values = key_values[:-2]
        params['key_values'] = key_values

        field_values = ''
        for col in from_table.columns:
            field_values += "'{0}: ' || COALESCE({0}::text, '') || ',' ||".format(col.name)
        field_values = field_values[:-10]
        params['field_values'] = field_values

        sql = """INSERT INTO {dv}._exceptions (_runid, schema, table_name, message, key_fields, fields )
SELECT {runid}, '{schema}', '{from_table}', _validation_msg, {key_values}, {field_values}
FROM {schema}.{from_table}
WHERE _runid = {runid} AND NOT _valid;""".format(
            **params)
        self.execute(sql, 'copy to exceptions_table <blue>' + params['from_table'] + '</>')



        # raise Exception(ex.args[0])


class EtlSourceToSor(BaseEtl):
    def __init__(self, pipe):
        super().__init__(pipe)


    # def source_to_sor(self, mappings):
    #     if isinstance(mappings.source, File):
    #         self.source_file_to_sor(mappings)
    #     elif isinstance(mappings.source, SourceTable):
    #         self.source_db_to_sor(mappings)
    def init_source_to_sor_old(self, mappings):
        params = mappings.__dict__
        params.update(self._get_fixed_params())
        params['fields'] = mappings.get_fields()
        params['key_fields'] = mappings.get_keys()
        params['tmp_fields'] = mappings.get_fields(alias='tmp')
        params['fields_compare'] = mappings.get_fields_compare(source_alias='tmp', target_alias='hstg')
        params['keys_compare'] = mappings.get_keys_compare(source_alias='tmp', target_alias='hstg')

        self.append_step('source to csv', self.source_to_csv_old, params)
        sql = "TRUNCATE TABLE {sor}.{temp_table}_hash;".format(**params)
        self.append_step('truncate temp', self.dwh.execute, sql)
        sql = "COPY {sor}.{temp_table}_hash ({key_fields}, _hash) FROM  '{file_name}' DELIMITER ';' CSV HEADER;".format(**params)
        self.append_step('copy into temp hash', self.dwh.execute, sql)
        sql = "SELECT COUNT(*) FROM {sor}.{sor_table};".format(**params)
        result = None
        self.append_step('get rowcount', self.dwh.execute_read, sql, result)
        #todo append voorwaarde & return
        self.append_condition(result > 0, yes = [], no = [] )
        self.append_step('', self.dwh.execute, sql)
        self.append_step('', self.dwh.execute, sql)
        self.append_step('', self.dwh.execute, sql)

    def source_to_csv_old(self, params):
        if isinstance(params['source'], SourceTable):
            file_name = params['source'].to_csv()
            params['file_name'] = file_name

    def source_to_sor(self, mappings):
        by_md5 = type(mappings.source) is SourceTable or type(mappings.source) is SourceQuery  # isinstance(mappings.source, SourceTable)
        if by_md5:
            self.source_to_sor_by_hash(mappings)
            return
        try:
            debug = 'debug' in self.pipe.pipeline.config and self.pipe.pipeline.config['debug']

            params = mappings.__dict__
            params.update(self._get_fixed_params())
            params['fields'] = mappings.get_fields()
            params['key_fields'] = mappings.get_keys()
            params['tmp_fields'] = mappings.get_fields(alias='tmp')
            params['fields_compare'] = mappings.get_fields_compare(source_alias='tmp', target_alias='hstg')
            params['keys_compare'] = mappings.get_keys_compare(source_alias='tmp', target_alias='hstg')
            params['encoding'] = mappings.get_source_encoding()

            # STAP 1
            if isinstance(mappings.source, SourceTable):
                file_name = mappings.source.to_csv(filter=mappings.filter, ignore_fields=mappings.ignore_fields, debug=debug)
                params['file_name'] = file_name
                # self.logger.log('  source to csv'.format(mappings))

            # STAP 2
            sql = "TRUNCATE TABLE {sor}.{temp_table};".format(**params)
            self.execute(sql, 'truncate {}'.format(params['temp_table']))

            # STAP 3 Bron data naar temp
            # we faken de quote voor textvelden opdat json velden (met dubbele quotes) goed worden ingelezen en later eenvoudig zijn te parsen naar jsonb
            sql = "COPY {sor}.{temp_table} ({fields}) FROM  '{file_name}' DELIMITER ';' CSV HEADER ENCODING '{encoding}' QUOTE '|';".format(**params)
            self.execute(sql, 'copy into {}'.format(params['temp_table']))

            # STAP 4a
            # sql = """INSERT INTO {sor}.{sor_table}(_runid, _insert_date, _hash, _revision, {fields})
            # SELECT {runid},now(), tmp._hash, 1, {fields}
            # FROM {sor}.{temp_table} tmp
            # WHERE NOT EXISTS (SELECT 1 FROM {sor}.{sor_table} hstg WHERE ({keys_compare}));""".format(**params)
            # self.execute(sql,  'insert new into sor')
            #
            # # STAP 4b
            # sql = """INSERT INTO {sor}.{sor_table}(_runid, _insert_date, _hash, _revision, {fields})
            #     SELECT {runid},now(), tmp._hash, hstg._revision + 1, {tmp_fields}
            #     FROM {sor}.{temp_table} tmp JOIN {sor}.{sor_table} hstg ON ({keys_compare})
            #     WHERE _active = True AND ({fields_compare});""".format(**params)
            # self.execute(sql,  'insert changed into sor')

            sql = """INSERT INTO {sor}.{sor_table}(_runid, _source_system, _insert_date, _hash, _active, {fields})
            SELECT {runid}, '{source_system}', now(), '', True, {fields}
            FROM {sor}.{temp_table} tmp
            EXCEPT
            SELECT {runid}, '{source_system}', now(), '', True, {fields}
            FROM {sor}.{sor_table} hstg WHERE _active = True;""".format(**params)
            self.execute(sql, 'insert new into {}'.format(params['sor_table']))

            # STAP 5
            params['keys_compare'] = mappings.get_keys_compare(source_alias='previous', target_alias='current')
            # oude is nog actief, maar runid is kleiner. Dit is het laatste record
            sql = """update {sor}.{sor_table} current set _revision = previous._revision + 1
                    from {sor}.{sor_table} previous where current._active = True AND previous._active = True AND ({keys_compare}) and previous._runid < current._runid;""".format(
                **params)
            self.execute(sql, 'update revision')
            # hierna pas oude inactief zetten
            # sql = """update {sor}.{sor_table} previous set _active = False, _finish_date = current._insert_date
            #         from {sor}.{sor_table} current where previous._active = True AND ({keys_compare}) and current._revision = (previous._revision + 1);""".format(
            #     **params)
            sql = """update {sor}.{sor_table} previous set _active = False, _finish_date = current._insert_date
                    from {sor}.{sor_table} current where previous._active = True AND ({keys_compare}) and previous._runid < current._runid;""".format(
                **params)
            self.execute(sql, 'update set old ones inactive')

            # sql = """update {sor}.{sor_table} current set _revision = previous._revision + 1
            #         from {sor}.{sor_table} previous where current._active = True AND ({keys_compare}) and previous._runid < current._runid;""".format(
            #     **params)
            # self.execute(sql,  'update sor set old ones inactive')

        except Exception as ex:
            self.logger.log_error(mappings.name, err_msg=ex.args[0])





    def source_to_sor_by_hash(self, mappings):
        """Eerst worden alleen hash-keys van bron database opgehaald. Deze worden vergeleken met huidige data.
        zijn hashes gewijzigd dan betreft het een gewijzigd record.
        Alleen van deze worden alle gegevens opgehaald"""
        try:
            debug = 'debug' in self.pipe.pipeline.config and self.pipe.pipeline.config['debug']

            params = mappings.__dict__
            params.update(self._get_fixed_params())
            params['fields'] = mappings.get_fields()
            params['key_fields'] = mappings.get_keys()
            params['tmp_fields'] = mappings.get_fields(alias='tmp')
            params['fields_compare'] = mappings.get_fields_compare(source_alias='tmp', target_alias='hstg')
            params['keys_compare'] = mappings.get_keys_compare(source_alias='tmp', target_alias='hstg')

              # STAP 1 data van database in csv file
            if isinstance(mappings.source, SourceTable):
                file_name = mappings.source.to_csv(md5_only=True, filter=mappings.filter, ignore_fields=mappings.ignore_fields, debug=debug)
                params['file_name'] = file_name
            # self.logger.log_simple('    source hash to csv'.format(mappings))

            # STAP 2
            sql = "TRUNCATE TABLE {sor}.{temp_table}_hash;".format(**params)
            self.execute(sql, 'truncate temp')

            # STAP 3 Bron data naar temp_hash
            sql = "COPY {sor}.{temp_table}_hash ({key_fields}, _hash) FROM  '{file_name}' DELIMITER ';' CSV HEADER;".format(
                    **params)
            self.execute(sql, 'copy into temp hash')

            # STAP 4a kijk of sor_table al data bevat zo ja dan wijzigingen bepalen. Zo nee dan alle data ophalen
            sql = "SELECT COUNT(*) FROM {sor}.{sor_table};".format(**params)
            result = self.execute_read(sql, 'get rowcount')
            rowcount = result[0][0]

            # STAP 6
            sql = "TRUNCATE TABLE {sor}.{temp_table};".format(**params)
            self.execute(sql, 'truncate temp')

            filter = ''
            i = 0
            if rowcount > 0:
                # STAP 4 bepaal wijzigingen van temp tov laatste sor data
                # CM : Vergelijk alleen met actieve records.
                sql = "UPDATE {sor}.{temp_table}_hash tmp SET _changed = TRUE WHERE NOT EXISTS (SELECT 1 FROM {sor}.{sor_table} hstg WHERE ({keys_compare}) AND tmp._hash = hstg._hash AND _active = True);".format(
                        **params)
                self.execute(sql, 'set status(changed) of temp')

                # STAP 5 haal alle data op uit bron
                # maak where IN

                sql = """SELECT {key_fields} FROM {sor}.{temp_table}_hash WHERE _changed;""".format(**params)
                # changed_keys = []
                changed_keys_str = ''
                rows = self.dwh.engine.execute(sql)
                # if len(rows) > 1000:
                #     debug = True
                for row in rows:
                    i += 1
                    keys_concat = ''.join(list(row))
                    changed_keys_str += "'{}',".format(keys_concat)
                    if i % 1000 == 0:

                        changed_keys_str = changed_keys_str[:-1]
                        key_concat = params['key_fields'].replace(',', '||')
                        filter = 'WHERE {} IN ({})'.format(key_concat, changed_keys_str)
                        print(filter)
                        file_name = mappings.source.to_csv(md5_only=False, filter=filter,  ignore_fields=mappings.ignore_fields, debug=debug)
                        params['file_name'] = file_name

                        sql = "COPY {sor}.{temp_table} ({fields}) FROM  '{file_name}' DELIMITER ';' CSV HEADER;".format(
                            **params)

                        self.execute(sql, 'copy into temp row {} - {}'.format(i, i+ 1000))

                        changed_keys_str = ''

                changed_keys_str = changed_keys_str[:-1]
                if changed_keys_str.strip():
                # changed_keys_str = ','.join(changed_keys)
                    key_concat = params['key_fields'].replace(',', '||')
                    filter = 'WHERE {} IN ({})'.format(key_concat, changed_keys_str)
                # self.logger.log_simple('    generated where in sql'.format(mappings))
                else:
                    # CM: zonder gewijzigde records nooit iets ophalen
                    filter= 'WHERE 1=2'
            else:
                filter = mappings.filter

            #STAP 5b
            if isinstance(mappings.source, SourceTable):
                file_name = mappings.source.to_csv(md5_only=False, filter=filter, ignore_fields=mappings.ignore_fields, debug=debug)
                params['file_name'] = file_name
            # self.logger.log_simple('    source complete to csv'.format(mappings))



            # STAP 7 Bron data naar temp
            sql = "COPY {sor}.{temp_table} ({fields}) FROM  '{file_name}' DELIMITER ';' CSV HEADER;".format(**params)
            self.execute(sql, 'copy into temp row {} - end'.format(i))

            # STAP 7a Update _hash
            params['tmp_fields'] = mappings.get_fields(alias='tmp')
            params['keys_compare'] = mappings.get_keys_compare(source_alias='tmp_hash', target_alias='tmp')

            sql = "UPDATE {sor}.{temp_table} tmp set _hash = tmp_hash._hash FROM {sor}.{temp_table}_hash tmp_hash WHERE  {keys_compare};".format(**params)
            self.execute(sql, 'update _hash in temp')

            # STAP 8  insert into sor
            sql = """INSERT INTO {sor}.{sor_table}(_runid, _source_system, _insert_date, _hash, {fields})
                SELECT {runid},'{source_system}', now(), tmp._hash, {tmp_fields}
                FROM {sor}.{temp_table} tmp
                 --JOIN {sor}.{temp_table}_hash tmp_hash ON {keys_compare};""".format(**params)
            self.execute(sql, 'insert new into sor')

            params['keys_compare'] = mappings.get_keys_compare(source_alias='previous', target_alias='current')

            # STAP 9a SET revisienummers
            sql = """UPDATE {sor}.{sor_table} current SET _revision = previous._revision + 1
                        --FROM (SELECT {key_fields}, max(_revision) as _revision, max(_runid) as _runid FROM {sor}.{sor_table} WHERE _active = TRUE GROUP BY {key_fields}) as previous
                        FROM {sor}.{sor_table} previous
                        WHERE current._active = True AND previous._active = True AND previous._runid < current._runid AND {keys_compare};
                        --current._revision = 0 AND {keys_compare};""".format(**params)
            self.execute(sql, 'update sor set _revision')

            # STAP 9b SET actieve records
            sql = """UPDATE {sor}.{sor_table} previous SET _active = False, _finish_date = current._insert_date
                        FROM {sor}.{sor_table} current WHERE previous._active = True AND ({keys_compare}) AND current._revision = (previous._revision + 1);""".format(
                **params)
            self.execute(sql, 'update sor set old ones inactive')


        except Exception as ex:
            self.logger.log_error(mappings.name, err_msg=ex.args[0])

    def validate_duplicate_keys(self, mappings, sor_schema):
        try:

            validation = SorValidation(tbl=mappings.sor_table, schema=sor_schema)
            validation.msg = 'duplicate key error'
            validation.set_check_for_duplicate_keys(mappings.get_keys())
            self.validate_duplicates(validation)

        except Exception as ex:
            self.logger.log_error(mappings.name, err_msg=ex.args[0])
            # raise Exception(ex.args[0])

    def validate_duplicates(self, validation: SorValidation):
        try:
            params = validation.__dict__
            params.update(self._get_fixed_params())
            params['keys'] = validation.get_keys()
            params['sor_table'] = validation.tbl.name

            sql = """UPDATE {sor}.{sor_table} set _validation_msg = '{msg}'
                where (_runid, {keys}) in (select _runid, {keys} from {sor}.{sor_table} GROUP BY _runid, {keys} HAVING count(*) > 1);""".format(
                **params)
            self.execute(sql, 'validate sor: mark duplicate keys')

            sql = """update {sor}.{sor_table} set _valid = FALSE, _validation_msg = '{msg}-first item'
                where _validation_msg = '{msg}' AND floor(_runid) = floor({runid}) AND _id in (select DISTINCT ON ({keys}) _id from {sor}.{sor_table} WHERE floor(_runid) = floor({runid}) ORDER BY {keys}, _id);""".format(
                **params)
            self.execute(sql, 'validate sor: mark first items of duplicate keys')

            # self.copy_to_exceptions_table(mappings.source)

        except Exception as ex:
            self.logger.log_error(validation.msg, err_msg=ex.args[0])
            # raise Exception(ex.args[0])

    def validate_sor(self, validation: SorValidation):
        try:
            if validation.check_for_duplicate_keys:
                self.validate_duplicates(validation)
            else:
                params = validation.__dict__
                params.update(self._get_fixed_params())
                params['sor_table'] = validation.tbl.name
                sql = """UPDATE {sor}.{sor_table} set _valid = False, _validation_msg = COALESCE(_validation_msg, '') || '{msg}; '
                    where _runid = {runid} AND {sql_condition};""".format(
                    **params)
                self.execute(sql, 'validate sor: ' + validation.msg)

            # self.copy_to_exceptions_table(validation.tbl)
        except Exception as ex:
            self.logger.log_error(validation.msg, err_msg=ex.args[0])


class EtlSorToDv(BaseEtl):
    def __init__(self, pipe):
        super().__init__(pipe)

    def sor_to_entity(self, mappings: 'SorToEntityMapping'):
        self.logger.log('START <blue>{}</>'.format(mappings), indent_level=3)
        try:
            if not mappings.filter:
                mappings.filter = '1=1'
            params = mappings.__dict__
            params.update(self._get_fixed_params())
            params['hub'] = mappings.target.cls_get_hub_name()
            params['hub_type'] = mappings.target.cls_get_hub_type()
            if mappings.type:
                params['hub_type'] = mappings.type
            params['sor_table'] = mappings.source.name
            params['relation_type'] = mappings.type
            params['filter_hub'] = params['filter']

            sql = "SELECT MAX(_id) FROM {dv}.{hub};".format(**params)
            result = self.execute_read(sql, 'get maxid')
            maxid = result[0][0]
            if maxid == None:
                maxid = 0
            if maxid > 0:
                filter_on_runid = 'floor(hstg._runid) = floor({runid})'.format(**params)
                params['filter_hub'] += ' AND ' + filter_on_runid
            self.hub_id = maxid

            if mappings.bk_mapping:
                sql = """SELECT DISTINCT 0 as _id, 0 as is_new, {runid}, now(), '{source_system}', '{hub_type}', {bk_mapping} as bk FROM {sor}.{sor_table} hstg WHERE hstg._valid AND ({bk_mapping}) IS NOT NULL AND {filter_hub};""".format(
                    **params)
                
                rows = self.execute_read(sql, 'get sor rows ')

                self.dwh.start_transaction()
                for row in rows:

                    hub_id, is_new = self.ensure_hub(row, params)
                    row['_id'] = hub_id
                    row['is_new'] = is_new

                    for sat_mappings in mappings.sat_mappings.values():
                        self.__sor_to_sat(row, params, sat_mappings)

            # elif mappings.key_mappings:
            #     target_key_sat = ''
            #     compare_key_fields = ''
            #     for key_mapping in mappings.key_mappings:
            #         target_key_sat = key_mapping.target.table.name
            #         compare_key_fields += "hstg.{} = key_sat.{} AND ".format(key_mapping.source, key_mapping.target)
            #     compare_key_fields = compare_key_fields[:-5]
            #     params['target_key_sat'] = target_key_sat
            #     params['compare_key_fields'] = compare_key_fields
            #     sql = """UPDATE {sor}.{sor_table} hstg SET fk_{relation_type}{hub} = hub._id FROM {dv}.{hub} hub, {dv}.{target_key_sat} key_sat WHERE hub._id = key_sat._id AND key_sat._active AND {compare_key_fields} AND hstg._valid AND {filter_hub};""".format(
            #         **params)
            #     self.execute(sql, 'update fk_hub in sor table')
            self.dwh.commit()
            self.logger.log('FINISH {}'.format(mappings), indent_level=3)
        except Exception as ex:
            self.logger.log_error(mappings.name, err_msg=ex.args[0])

    def __sor_to_sat(self, row, params, sat_mappings):
        self.logger.log('START {}'.format(sat_mappings), indent_level=4)
        satparams = sat_mappings.__dict__
        satparams.update(self._get_fixed_params())
        sat_cls = sat_mappings.target
        satparams['sat'] = sat_cls.cls_get_name()
        if 'hub' in params:
            satparams['hub_or_link'] = params['hub']
            satparams['relation_type'] = params['relation_type']
        else:
            satparams['hub_or_link'] = params['link']
            satparams['relation_type'] = ''
        satparams['filter'] = params['filter']
        satparams['sor_fields'] = sat_mappings.get_source_fields(alias='hstg')
        satparams['sat_fields'] = sat_mappings.get_sat_fields()
        satparams['fields_compare'] = sat_mappings.get_fields_compare(source_alias='hstg', target_alias='sat')

        if sat_cls.__base__ == HybridSat:
            pass
        else:
            if row['is_new']:
                satparams['id'] = row['_id']
                #sor_fields moet uit rij komen
                sql = """INSERT INTO {dv}.{sat} (_id, _runid, _source_system, _insert_date, _revision, {sat_fields})
                                    VALUES ( {id}, {runid}, '{source_system}', now(), 0, {sor_fields})""".format(**satparams)
                self.execute_without_commit(sql)
            else:
                pass
                #kijken of er verschil is
                #zo ja updaten en oude krijgt revision


        self.logger.log('FINISH {}'.format(sat_mappings), indent_level=4)

    def __sor_to_sat_old(self, params, sat_mappings):
        self.logger.log('    START {}'.format(sat_mappings))
        satparams = sat_mappings.__dict__
        satparams.update(self._get_fixed_params())
        sat_cls = sat_mappings.target
        if 'hl7' in sat_cls.cls_get_name():
            debug = True
        satparams['sat'] = sat_cls.cls_get_name()
        if 'hub' in params:
            satparams['hub_or_link'] = params['hub']
            satparams['relation_type'] = params['relation_type']
        else:
            satparams['hub_or_link'] = params['link']
            satparams['relation_type'] = ''
        satparams['filter'] = params['filter']
        satparams['sor_fields'] = sat_mappings.get_source_fields(alias='hstg')
        satparams['sat_fields'] = sat_mappings.get_sat_fields()
        satparams['fields_compare'] = sat_mappings.get_fields_compare(source_alias='hstg', target_alias='sat')
        sql = "SELECT COUNT(*) FROM {dv}.{sat};".format(**satparams)
        result = self.execute_read(sql, 'get rowcount')
        rowcount = result[0][0]
        if sat_cls.__base__ == HybridSat:
            if rowcount == 0:
                # alle zijn nieuw; geen rekening houden met _runid
                sql = """INSERT INTO {dv}.{sat} (_id, _runid, type, _source_system, _insert_date, _revision, {sat_fields})
                        SELECT DISTINCT ON (fk_{relation_type}{hub_or_link}) fk_{relation_type}{hub_or_link}, {runid}, '{type}', '{source_system}', now(), 1, {sor_fields}
                        FROM {sor}.{sor_table} hstg
                        WHERE hstg._valid AND hstg.fk_{relation_type}{hub_or_link} IS NOT NULL AND NOT EXISTS (select 1 from {dv}.{sat} sat where sat._id  =  _fk_{relation_type}{hub_or_link} AND sat.type = '{type}') AND {filter};""".format(
                    **satparams)
                self.execute(sql, '  insert new in sat')
            else:
                sql = """INSERT INTO {dv}.{sat} (_id, _runid, type, _source_system, _insert_date, _revision, {sat_fields})
                        SELECT DISTINCT ON (fk_{relation_type}{hub_or_link}) fk_{relation_type}{hub_or_link}, {runid}, '{type}', '{source_system}', now(), 1, {sor_fields}
                        FROM {sor}.{sor_table} hstg
                        WHERE floor(hstg._runid) = floor({runid}) AND hstg._valid AND hstg.fk_{relation_type}{hub_or_link} IS NOT NULL AND NOT EXISTS (select 1 from {dv}.{sat} sat where sat._id  =  fk_{relation_type}{hub_or_link} AND sat.type = '{type}') AND {filter};""".format(
                    **satparams)
                self.execute(sql, '  insert new in sat')

                sql = """INSERT INTO {dv}.{sat} (_id, _runid, _source_system, _insert_date, _revision, {sat_fields})
                        SELECT DISTINCT ON (fk_{relation_type}{hub_or_link}) fk_{relation_type}{hub_or_link}, {runid}, '{source_system}', now(), sat._revision + 1, {sor_fields}
                        FROM {sor}.{sor_table} hstg JOIN {dv}.{sat} sat ON sat._id =  fk_{relation_type}{hub_or_link}
                        WHERE floor(hstg._runid) = floor({runid}) AND hstg._valid AND hstg.fk_{relation_type}{hub_or_link} IS NOT NULL AND sat._active = True AND sat.type = '{type}' AND ({fields_compare}) AND {filter};""".format(
                    **satparams)
                self.execute(sql, '  insert changed in sat')

                sql = """UPDATE {dv}.{sat} previous SET _active = FALSE, _finish_date = current._insert_date
                        FROM {dv}.{sat} current WHERE previous._active = TRUE AND previous._id = current._id AND current._revision = (previous._revision + 1) AND current.type = '{type}' AND previous.type = '{type}' ;""".format(
                    **satparams)
                self.execute(sql, '  update sat set old ones inactive')
                self.logger.log('    FINISH {}'.format(sat_mappings))
        else:

            if rowcount == 0:
                # alle zijn nieuw; geen rekening houden met _runid
                sql = """INSERT INTO {dv}.{sat} (_id, _runid, _source_system, _insert_date, _revision, {sat_fields})
                        SELECT DISTINCT ON (fk_{relation_type}{hub_or_link}) fk_{relation_type}{hub_or_link}, {runid}, '{source_system}', now(), 1, {sor_fields}
                        FROM {sor}.{sor_table} hstg
                        WHERE hstg._valid AND hstg.fk_{relation_type}{hub_or_link} IS NOT NULL AND NOT EXISTS (select 1 from {dv}.{sat} sat where sat._id  =  _fk_{relation_type}{hub_or_link}) AND {filter};""".format(
                    **satparams)
                self.execute(sql, '  insert new in sat')
            else:
                sql = """INSERT INTO {dv}.{sat} (_id, _runid, _source_system, _insert_date, _revision, {sat_fields})
                        SELECT DISTINCT ON (fk_{relation_type}{hub_or_link}) fk_{relation_type}{hub_or_link}, {runid}, '{source_system}', now(), 1, {sor_fields}
                        FROM {sor}.{sor_table} hstg
                        WHERE floor(hstg._runid) = floor({runid}) AND hstg._valid AND hstg.fk_{relation_type}{hub_or_link} IS NOT NULL AND NOT EXISTS (select 1 from {dv}.{sat} sat where sat._id  =  _fk_{relation_type}{hub_or_link}) AND {filter};""".format(
                    **satparams)
                self.execute(sql, '  insert new in sat')

                sql = """INSERT INTO {dv}.{sat} (_id, _runid, _source_system, _insert_date, _revision, {sat_fields})
                        SELECT DISTINCT ON (fk_{relation_type}{hub_or_link}) fk_{relation_type}{hub_or_link}, {runid}, '{source_system}', now(), sat._revision + 1, {sor_fields}
                        FROM {sor}.{sor_table} hstg JOIN {dv}.{sat} sat ON sat._id =  fk_{relation_type}{hub_or_link}
                        WHERE floor(hstg._runid) = floor({runid}) AND hstg._valid AND hstg.fk_{relation_type}{hub_or_link} IS NOT NULL AND sat._active = True AND ({fields_compare}) AND {filter};""".format(
                    **satparams)
                self.execute(sql, '  insert changed in sat')

                sql = """UPDATE {dv}.{sat} previous SET _active = FALSE, _finish_date = current._insert_date
                        FROM {dv}.{sat} current WHERE previous._active = TRUE AND previous._id = current._id AND current._revision = (previous._revision + 1);""".format(
                    **satparams)
                self.execute(sql, '  update sat set old ones inactive')
                self.logger.log('    FINISH {}'.format(sat_mappings))

    def sor_to_link(self, mappings):
        self.logger.log('  START {}'.format(mappings))
        try:
            if not mappings.filter:
                mappings.filter = '1=1'
            params = mappings.__dict__
            params.update(self._get_fixed_params())
            params['link'] = mappings.target.cls_get_name()
            params['link_type'] = mappings.type
            params['sor_table'] = mappings.source.name
            params['source_fks'] = self.__get_link_source_fks(mappings)
            params['source_fks_is_not_null'] = self.__get_link_source_fks_is_not_null(mappings)
            params['source_fks_is_null'] = params['source_fks_is_not_null'].replace('NOT ', '')
            params['target_fks'] = self.__get_link_target_fks(mappings)

            params['join'] = self.__get_link_join(mappings, schema_name=self.pipe.sor.name)
            params['fks_compare'] = self.__get_link_fks_compare(mappings, source_alias='hstg', target_alias='link')

            sql = """
            INSERT INTO {dv}.{link} (_runid, _source_system, _insert_date, type, {target_fks})
            SELECT DISTINCT {runid}, '{source_system}', now(), '{link_type}', {source_fks}
            FROM {sor}.{sor_table} hstg {join}
            WHERE floor(hstg._runid) = floor({runid})
            AND NOT ({source_fks_is_null})
            AND NOT EXISTS (SELECT 1 FROM {dv}.{link} link WHERE {fks_compare} AND link.type='{link_type}') AND {filter};""".format(**params)
            self.execute(sql,  'insert new links')

            if len(mappings.sat_mappings) > 0:
                # todo refactor
                from_sql = ''
                where_sql = ''
                for field_mapping in mappings.field_mappings:
                    join = field_mapping.join
                    if field_mapping.bk:
                        join_tbl = field_mapping.get_source_table()
                        from_sql += ', dv.{}'.format(join_tbl)
                        where_sql += 'dv.{}.bk = {} AND '.format(join_tbl, field_mapping.bk)
                # from_sql = from_sql[:-1]
                where_sql = where_sql[:-5]

                params['from'] = from_sql
                params['where'] = where_sql

                sql = """UPDATE {sor}.{sor_table} hstg SET fk_{type}{link} = link._id
FROM {dv}.{link} link {from}
WHERE {fks_compare} AND {where}
AND hstg._valid AND {filter};""".format(
                    **params)

                self.execute(sql, 'update fk_link in sor table')

            for sat_mappings in mappings.sat_mappings.values():
                self.__sor_to_sat(params, sat_mappings)
            self.logger.log('  FINISH {}'.format(mappings))
        except Exception as ex:
            self.logger.log_error(mappings.name, err_msg=ex.args[0])

    def __get_link_source_fks(self, mappings: 'SorToLinkMapping'):
        fks = ''
        for field_mapping in mappings.field_mappings:
            if field_mapping.source.table == mappings.sor_table_name:
                fks += '{}, '.format(field_mapping.source)
            else:
                # fks += '{}.{}, '.format(field_mapping.source.table, field_mapping.source)
                fks += '{}.{}, '.format(field_mapping.source_alias, field_mapping.source)
        fks = fks[:-2]
        return fks

    def __get_link_source_fks_is_not_null(self, mappings: 'SorToLinkMapping'):
        fks = ''
        for field_mapping in mappings.field_mappings:
            if field_mapping.source.table == mappings.sor_table_name:
                fks += '{} IS NOT NULL AND '.format(field_mapping.source)
            else:
                fks += '{}.{} IS NOT NULL AND '.format(field_mapping.source_alias, field_mapping.source)
        fks = fks[:-5]
        return fks

    def __get_link_target_fks(self, mappings: 'SorToLinkMapping'):
        fks = ''
        for field_mapping in mappings.field_mappings:
            fks += '{}, '.format(field_mapping.target)
        fks = fks[:-2]
        return fks

    def __get_link_fks_compare(self, mappings: 'SorToLinkMapping', source_alias='', target_alias=''):
        if not source_alias: source_alias = 'hstg'
        if not target_alias: target_alias = 'link'
        fks_compare = ''
        for field_mapping in mappings.field_mappings:
            if isinstance(field_mapping.source, ConstantValue):
                fks_compare += '{0} = {1}.{2} AND '.format(field_mapping.source, target_alias, field_mapping.target)
            elif field_mapping.is_view_mapping:
                fks_compare += '{0}.{1} = {2}.{3} AND '.format(field_mapping.source.table, field_mapping.source, target_alias, field_mapping.target)
            else:
                source_alias = self.__get_link_alias_of_source_tbl(field_mapping, fks_compare)
                fks_compare += '{0}.{2} = {1}.{3} AND '.format(source_alias, target_alias, field_mapping.source, field_mapping.target)
        fks_compare = fks_compare[:-4]
        return fks_compare

    def __get_link_join(self, mappings, schema_name='sor'):
        #todo robuuster maken met aliassen en ands
        join_sql = ''
        for field_mapping in mappings.field_mappings:
            join = field_mapping.join
            bk = field_mapping.bk
            if join:
                join_tbl = field_mapping.get_source_table()
                if field_mapping.is_view_mapping:
                    schema_name = 'dv'
                    join_sql += ' INNER JOIN {}.{} ON {} \r\n'.format(schema_name, join_tbl, join)
                else:
                    join_alias = self.__get_link_alias_of_source_tbl(field_mapping, join_sql)
                    join = join.replace(join_tbl + '.', join_alias + '.')
                    join_sql += ' INNER JOIN {}.{} {} ON {} \r\n'.format(schema_name, join_tbl, join_alias, join)
            elif bk:
                join_tbl = field_mapping.get_source_table()
                join_alias = self.__get_link_alias_of_source_tbl(field_mapping, join_sql)
                join_sql += ' LEFT JOIN dv.{0} AS {1} ON {1}.bk = {2} \r\n'.format(join_tbl, join_alias, bk)
        return join_sql

    def __get_link_alias_of_source_tbl(self, field_mapping, total_sql, default_alias='hstg'):
        #bij een join in de sql moet elke bron tabel een eigen alias krijgen
        #dat wordt hier bepaald
        alias = default_alias
        if field_mapping.join:
            join_tbl = field_mapping.get_source_table()
            alias = join_tbl[:3] + '_hstg'
            index = 1
            while alias in total_sql:
                alias = alias + str(index)
                index += 1
        if field_mapping.bk:
            # join_tbl = field_mapping.get_source_table()
            alias = field_mapping.source_alias
        return alias

    def sor_to_ref(self, mappings: SorToValueSetMapping):
        self.logger.log('  START {}'.format(mappings))
        try:
            params = mappings.__dict__
            params.update(self._get_fixed_params())

            if isinstance(mappings.source, dict):
                values = ''
                for code, descr in mappings.source.items():
                    params['code'] = code
                    params['descr'] = descr
                    values += "('{code}', '{descr}'),\r\n".format(**params)
                values = values [:-3]
                params['values'] = values
                insert_sql = """

                    CREATE TEMP TABLE _ref_values_temp (code text, weergave_naam text);

                    INSERT INTO _ref_values_temp (code, weergave_naam)
                    VALUES {values};

                    INSERT INTO {dv}._ref_valuesets (_runid, _source_system, _insert_date, naam, oid)
                    SELECT {runid}, '{source_system}', now(), '{ref_type}', NULL
                    WHERE NOT EXISTS (SELECT 1 FROM {dv}._ref_valuesets sets WHERE sets.naam = '{ref_type}');

                    INSERT INTO {dv}._ref_values (_runid, _active, _source_system, _insert_date, _revision, valueset_naam, code, weergave_naam)
                    SELECT DISTINCT {runid}, True, '{source_system}', now(), 0, '{ref_type}', code, weergave_naam
                    FROM _ref_values_temp tmp
                    WHERE
                      NOT EXISTS (SELECT 1 FROM {dv}._ref_values ref WHERE ref.valueset_naam = '{ref_type}' AND ref.code = tmp.code AND ref.weergave_naam = tmp.weergave_naam);


                    DROP TABLE _ref_values_temp;
                      """.format(**params)
            elif mappings.source_type_field:
                insert_sql = """
                    INSERT INTO {dv}._ref_valuesets (_runid, _source_system, _insert_date, naam, oid)
                    SELECT DISTINCT {runid}, '{source_system}', now(), {source_type_field}, {source_oid_field}
                    FROM {sor}.{sor_table} hstg
                    WHERE floor(hstg._runid) = floor({runid})
                      AND hstg._valid AND hstg._active
                      AND NOT EXISTS (SELECT 1 FROM {dv}._ref_valuesets sets WHERE sets.naam = hstg.{source_type_field});

                    INSERT INTO {dv}._ref_values (_runid, _active, _source_system, _insert_date, _revision, valueset_oid, valueset_naam, code, weergave_naam, niveau, niveau_type)
                    SELECT DISTINCT {runid}, True, '{source_system}', now(), 0, {source_oid_field}, {source_type_field}, {source_code_field}, {source_descr_field}, {source_level_field}, {source_leveltype_field}
                    FROM {sor}.{sor_table} hstg
                    WHERE floor(hstg._runid) = floor({runid})
                      AND hstg._valid AND hstg._active
                      AND NOT EXISTS (SELECT 1 FROM {dv}._ref_values ref WHERE ref.valueset_naam = hstg.{source_type_field} AND ref.code = hstg.{source_code_field}
                                AND ref.weergave_naam = hstg.{source_descr_field} AND ref.niveau = {source_level_field});""".format(**params)
            elif mappings.source_code_field and not mappings.source_descr_field:
                insert_sql = """
                    INSERT INTO {dv}._ref_valuesets (_runid, _source_system, _insert_date, naam, oid)
                    SELECT {runid}, '{source_system}', now(), '{ref_type}', NULL
                    WHERE NOT EXISTS (SELECT 1 FROM {dv}._ref_valuesets sets WHERE sets.naam = '{ref_type}');

                    INSERT INTO {dv}._ref_values (_runid, _active, _source_system, _insert_date, _revision, valueset_naam, code, weergave_naam, niveau, niveau_type)
                    SELECT DISTINCT {runid}, True, '{source_system}', now(), 0, '{ref_type}', {source_code_field}, NULL, NULL, NULL,
                    FROM {sor}.{sor_table} hstg
                    WHERE floor(hstg._runid) = floor({runid})
                      AND hstg._valid AND hstg._active
                      AND NOT EXISTS (SELECT 1 FROM {dv}._ref_values ref WHERE ref.valueset_naam = '{ref_type}' AND ref.code = hstg.{source_code_field}
                                    AND ref.weergave_naam = hstg.{source_descr_field} AND ref.niveau = {source_level_field} AND ref.niveau_type = {source_leveltype_field)};""".format(**params)
            else:
                insert_sql = """
                    INSERT INTO {dv}._ref_valuesets (_runid, _source_system, _insert_date, naam, oid)
                    SELECT {runid}, '{source_system}', now(), '{ref_type}', NULL
                    WHERE NOT EXISTS (SELECT 1 FROM {dv}._ref_valuesets sets WHERE sets.naam = '{ref_type}');

                    INSERT INTO {dv}._ref_values (_runid, _active, _source_system, _insert_date, _revision, valueset_naam, code, weergave_naam, niveau, niveau_type))
                    SELECT DISTINCT {runid}, True, '{source_system}', now(), 0, '{ref_type}', {source_code_field}, {source_descr_field}, {source_level_field}, {source_leveltype_field}
                    FROM {sor}.{sor_table} hstg
                    WHERE floor(hstg._runid) = floor({runid})
                      AND hstg._valid AND hstg._active
                      AND NOT EXISTS (SELECT 1 FROM {dv}._ref_values ref WHERE ref.valueset_naam = '{ref_type}' AND ref.code = hstg.{source_code_field}
                                    AND ref.weergave_naam = {source_descr_field} AND ref.niveau = {source_level_field} AND ref.niveau_type = {source_leveltype_field);""".format(**params)

            self.execute(insert_sql, 'insert refs')

            # oude is nog actief, maar runid is kleiner. Dit is het laatste record
            insert_sql = """update {dv}._ref_values current set _revision = previous._revision + 1
                    from {dv}._ref_values previous where current._active = True AND previous._active = True AND previous.valueset_naam = current.valueset_naam AND previous.code = current.code and previous._runid < current._runid;""".format(
                **params)
            self.execute(insert_sql, 'update ref revision')
            # nu oude inctief maken
            insert_sql = """update {dv}._ref_values previous set _active = False, _finish_date = current._insert_date
                    from {dv}._ref_values current where previous._active = True AND previous.valueset_naam = current.valueset_naam AND previous.code = current.code and previous._runid < current._runid;""".format(
                **params)
            self.execute(insert_sql, 'update ref set old ones inactive')
            self.logger.log('  FINISH {}'.format(mappings))
        except Exception as ex:
            self.logger.log_error(mappings.name, err_msg=ex.args[0])
            
    def view_to_entity(self, mappings):
        self.logger.log('  START {}'.format(mappings))
        try:
            if not mappings.filter:
                mappings.filter = '1=1'
            params = mappings.__dict__
            params.update(self._get_fixed_params())

            sql = """
          INSERT INTO {dv}.{hub} (_runid, _insert_date, _source_system, type, bk)
          SELECT DISTINCT {runid}, now(), '{source_system}', '{type}', {bk_mapping} FROM {dv}.{view} view
          WHERE view._valid AND {bk_mapping} NOT IN (SELECT bk FROM {dv}.{hub}) AND {filter};""".format(
                **params)
            self.execute(sql,  'insert new hub')

            # sql = """SELECT hub._id FROM {dv}.{hub} hub JOIN {dv}.{sor_table} view ON {bk_mapping} = hub.bk WHERE floor(view._runid) = floor({runid}) AND view._valid AND {filter};""".format(
            #     **params)
            # self.execute(sql,  'load hub_ids in mem (performance)')

            # sql = """UPDATE {dv}.{sor_table} view SET _fk_{type}{hub} = hub._id FROM {dv}.{hub} hub WHERE {bk_mapping} = hub.bk AND floor(view._runid) = floor({runid}) AND view._valid AND {filter};""".format(
            #     **params)
            # self.execute(sql,  'update fk_hub in sor table')

            for sat_mappings in mappings.sat_mappings.values():
                self.logger.log('    START {}'.format(sat_mappings))

                satparams = sat_mappings.__dict__
                satparams.update(self._get_fixed_params())
                satparams['view'] = sat_mappings.source
                satparams['type'] = params['type']
                satparams['filter'] = params['filter']
                satparams['bk_mapping'] = params['bk_mapping']
                satparams['view_fields'] = sat_mappings.get_source_fields(alias='view')
                satparams['sat_fields'] = sat_mappings.get_sat_fields()
                satparams['fields_compare'] = sat_mappings.get_fields_compare(source_alias='view', target_alias='sat')

                sql = """
                INSERT INTO {dv}.{sat} (_id, _runid, _source_system, _insert_date, _revision, {sat_fields})
                SELECT DISTINCT ON (hub._id) hub._id, {runid}, '{source_system}', now(), 1, {view_fields}
                FROM {dv}.{view} view
                JOIN {dv}.{hub} hub ON hub.bk = {bk_mapping}
                WHERE view._valid AND NOT EXISTS (select 1 from {dv}.{sat} sat where sat._id  =  hub._id) AND {filter};""".format(
                    **satparams)
                self.execute(sql,  '  insert new in sat')

                sql = """
                INSERT INTO {dv}.{sat} (_id, _runid, _source_system, _insert_date, _revision, {sat_fields})
                SELECT DISTINCT ON (hub._id) hub._id, {runid}, '{source_system}', now(), sat._revision + 1, {view_fields}
                FROM {dv}.{view} view
                JOIN {dv}.{hub} hub ON hub.bk = view.bk
                JOIN {dv}.{sat} sat ON sat._id =  hub._id
                WHERE view._valid AND sat._active = True AND ({fields_compare}) AND {filter};""".format(
                    **satparams)
                self.execute(sql,  '  insert changed in sat')

                sql = """
                UPDATE {dv}.{sat} previous SET _active = FALSE, _finish_date = current._insert_date
                FROM {dv}.{sat} current WHERE previous._active = TRUE AND previous._id = current._id AND current._revision = (previous._revision + 1);""".format(
                    **satparams)
                self.execute(sql,  '  update sat set old ones inactive')
                self.logger.log('    FINISH {}'.format(sat_mappings))
            self.logger.log('  FINISH {}'.format(mappings))
        except Exception as ex:
            self.logger.log_error(mappings.name, err_msg=ex.args[0])

    def view_to_link(self, mappings):
        self.logger.log('  START {}'.format(mappings))
        try:
            if not mappings.filter:
                mappings.filter = '1=1'
            params = mappings.__dict__
            params.update(self._get_fixed_params())
            params['source_fks'] = mappings.get_source_fks()
            params['target_fks'] = mappings.get_target_fks()
            params['from'] = mappings.get_from()
            params['join'] = mappings.get_join()
            params['fks_compare'] = mappings.get_fks_compare(target_alias='link')

            sql = """
            INSERT INTO {dv}.{link} (_runid, _source_system, _insert_date, {target_fks})
            SELECT {runid}, '{source_system}', now(), {source_fks}
            FROM {dv}.{view}  {join}
            WHERE floor({dv}.{view}._runid) = floor({runid}) AND
              NOT EXISTS (SELECT 1 FROM {dv}.{link} link WHERE {fks_compare}) AND {filter};""".format(**params)

            # sql = """
            # INSERT INTO {dv}.{link} (_runid, _source_system, _insert_date, {target_fks})
            # SELECT {runid}, '{source_system}', now(), {source_fks}
            # FROM {from}
            # WHERE {join} AND floor({dv}.{view}._runid) = floor({runid}) AND
            #   NOT EXISTS (SELECT 1 FROM {dv}.{link} link WHERE {fks_compare}) AND {filter};""".format(**params)

            self.execute(sql,  'insert new links')
            self.logger.log('  FINISH {}'.format(mappings))
        except Exception as ex:
            self.logger.log_error(mappings.name, err_msg=ex.args[0])

    def validate_dv(self, validation: DvValidation):
        try:
            params = validation.__dict__
            params.update(self._get_fixed_params())
            params['table'] = validation.table.name
            sql = """UPDATE {dv}.{table} set _valid = False, _validation_msg = COALESCE(_validation_msg, '') || '{msg}; '
                where _runid = {runid} AND {sql_condition};""".format(
                **params)
            self.execute(sql, 'validate dv: ' + validation.msg)

            self.copy_to_exceptions_table(validation.table, self.dwh.dv)
        except Exception as ex:
            self.logger.log_error(validation.msg, err_msg=ex.args[0])
            # raise Exception(ex.args[0])

    def ensure_hub(self, row, params):
        params['bk'] = row['bk']
        sql = """SELECT _id FROM {dv}.{hub} WHERE bk = '{bk}'""".format(**params)
        rows = self.execute_read(sql)
        id = 0
        is_new = False
        if rows :
            id = rows[0][0]
        if not id:
            id =  self.insert_hub(row, params)
            is_new = True
        return id, is_new

    def insert_hub(self, row, params):
        self.hub_id += 1
        params['id'] = self.hub_id
        sql = """INSERT INTO {dv}.{hub} (_id, _runid, type, bk) VALUES ({id}, {runid}, '{hub_type}', '{bk}')""".format(**params)
        rows = self.execute_without_commit(sql)
        return self.hub_id

