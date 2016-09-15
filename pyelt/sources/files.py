import csv

from pyelt.datalayers.database import Column


class File():
    def __init__(self, file_name):
        self.name = file_name[file_name.rfind('/') + 1:]
        self.file_name = file_name
        self.key_names = []
        self.columns = []
        self.is_reflected = False
        self.encoding = 'utf8'

    def __repr__(self):
        return self.name

    def set_primary_key(self, key_names=[]):
        key_names = [key.lower() for key in key_names]
        if not self.is_reflected:
            self.reflect()
        self.key_names = key_names
        for col in self.columns:
            col.is_key = (col.name in key_names)

    def primary_keys(self):
        return self.key_names  # [col.name for col in self.columns if col.is_key]

    def field_names(self):
        return [col.name for col in self.columns]

    def reflect(self):
        pass

    def get_header(self):
        if not self.is_reflected:
            self.reflect()
        return self.columns

class CsvFile(File):
    def __init__(self, file_name, **kwargs):
        super().__init__(file_name )
        self.file_kwargs = {}
        self.csv_kwargs = {}
        for k,v in kwargs.items():
            if k == 'encoding':
                self.file_kwargs[k] = v
                self.encoding = v
            elif k == 'delimiter':
                self.csv_kwargs[k] = v
        self.kwargs = kwargs

    def reflect(self):
        # lees eerste regel
        with open(self.file_name, 'r', **self.file_kwargs) as csvfile:
            reader = csv.reader(csvfile, **self.csv_kwargs)
            column_names = next(reader, None)
            for name in column_names:
                name = Column.clear_name(name)
                col = Column(name.lower())
                self.columns.append(col)
        self.is_reflected = True


class FixedLengthFile(File):
    def __init__(self, file_name, import_def):
        super().__init__(file_name )
        self.import_def = import_def
        if self.import_def:
            self.reflect()
            self.primary_key_from_import_def()

    def reflect(self):
        for field_def in self.import_def:
            field_name = field_def[0]
            field_name = Column.clear_name(field_name)
            col = Column(field_name.lower())
            self.columns.append(col)
        self.is_reflected = True

    def primary_key_from_import_def(self):
        key_names = []
        for field_def in self.import_def:
            field_name = field_def[0]
            is_key = len(field_def) > 2 and field_def[2]
            if is_key:
                key_names.append(field_name)
        self.set_primary_key(key_names)

