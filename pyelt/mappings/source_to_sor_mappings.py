from typing import Union, cast, List

from pyelt.datalayers.database import Table
from pyelt.mappings.transformations import FieldTransformation
from pyelt.sources.databases import SourceTable, SourceQuery
from pyelt.sources.files import File
from pyelt.mappings.base import BaseTableMapping


class SourceToSorMapping(BaseTableMapping):
    def __init__(self, source: Union['SourceTable', 'SourceQuery', 'File'], target: Union[str, Table], auto_map: bool = True, filter='', ignore_fields: List[str] = []) -> None:
        #todo transformations
        if isinstance(source, File):
            self.file_name = source.file_name
        elif isinstance(source, SourceTable):
            self.source_table = source
        elif isinstance(source, SourceQuery):
            self.source_table = source

        if isinstance(target, str):
            self.sor_table = str(target) #type: str
        else:
            target_tbl = cast(Table, target)
            self.sor_table = target_tbl.name
        super().__init__(source, target, filter)
        self.temp_table = self.sor_table.replace('_hstage', '') + '_temp'
        #todo keys
        self.keys = [] #type: List[str]
        ignore_fields = [s.lower() for s in ignore_fields]
        self.ignore_fields = ignore_fields
        # self.field_mappings = [] #type: List[FieldMapping]
        self.auto_map = auto_map
        if auto_map: self.create_auto_mappings(source, ignore_fields)



    def map_field(self, source: str, target: str = '', transform_func: 'FieldTransformation'=None, ref: str = '') -> None:
        super().map_field(source, target, transform_func)
        if source in self.source.key_names:
            self.keys.append(source)

    # def map_pk(self, source: str, target: str = '', transform_func: 'FieldTransformation'=None, ref: str = '') -> None:

    def create_auto_mappings(self, source: Union['SourceTable', 'SourceQuery', 'File'], ignore_fields: List[str] = []):
        if isinstance(source, File):
            field_names = source.get_header()
            for fld_name in field_names:
                if fld_name in ignore_fields:
                    continue
                self.map_field(fld_name, fld_name)
        elif isinstance(source, SourceQuery):
            if not source.is_reflected:
                source.reflect()
            for col in source.columns:
                if col.name in ignore_fields:
                    continue
                self.map_field(col.name, col.name)
        elif isinstance(source, SourceTable):
            if not source.is_reflected:
                source.reflect()
            for col in source.columns:
                if col.name in ignore_fields:
                    continue
                self.map_field(col.name, col.name)
        self.keys = [name.lower() for name in source.key_names]


    def get_fields(self, alias: str = '') -> str:
        return super().get_target_fields(alias)

    def get_fields_compare(self, source_alias: str='', target_alias: str='') -> str:
        if not source_alias: source_alias = 'tmp'
        if not target_alias: target_alias = 'hstg'
        return super().get_fields_compare(source_alias, target_alias)

    def get_keys(self, alias: str='') -> str:
        if not alias:
            return ','.join(self.keys)
        else:
            keys = ''
            for key in self.keys:
                keys += '{}.{},'.format(alias, key)
                keys = keys[:-1]
            return keys

    def get_keys_compare(self,    source_alias: str = '', target_alias: str = '') -> str:
        if not source_alias: source_alias = 'tmp'
        if not target_alias: target_alias = 'hstg'
        keys_compare = ''
        for key in self.keys:
            keys_compare += '{0}.{2} = {1}.{2} AND '.format(source_alias, target_alias, key)
        keys_compare = keys_compare[:-4]
        return keys_compare

    def get_source_encoding(self):
        #postgres valid name
        file_encoding = 'UTF-8'
        if isinstance(self.source, File):
            file_encoding = self.source.encoding.lower()
            if file_encoding == 'ascii' or file_encoding == 'latin-1':
                # conversie naar geldige postgres var
                file_encoding = 'LATIN1'
        return file_encoding

    def get_delimiter(self):
        delimiter = ';'
        if isinstance(self.source, File):
            delimiter = self.source.delimiter.lower() or ';'
        return delimiter

    def get_quote(self):
        quote = '|'
        if isinstance(self.source, File):
            quote = self.source.quote.lower() or '|'
        return quote

