import unittest

from pyelt.datalayers.database import Columns
from pyelt.datalayers.dv import DvEntity, Sat


class Foo(DvEntity):
    class Default(Sat):
        test1 = Columns.TextColumn()
        test1a = Columns.TextColumn()
        test1b = Columns.TextColumn()

    class Bar(Sat):
        test2 = Columns.TextColumn('test2a')

class Baz(Foo):
    #Dit zou error moeten geven, want super() kent ook al Default
    class Default(Sat):
        test3 = Columns.TextColumn()

    class Qux(Sat):
        test4 = Columns.TextColumn()

class Alone(Sat):
    test1 = Columns.TextColumn()

class TestDV(unittest.TestCase):
    """testen van pyelt/databases/dv.py"""
    def test_sat_name(self):
        self.assertEqual(Foo.Default.cls_get_name(), 'foo_sat')
        self.assertEqual(Foo.Bar.cls_get_name(), 'foo_sat_bar')
        self.assertEqual(Baz.Qux.cls_get_name(), 'foo_sat_qux')
        self.assertEqual(Baz.Default.cls_get_name(), 'foo_sat')
        self.assertRaises(Exception, Alone.cls_get_name)

    def test_sat_short_name(self):
        self.assertEqual(Foo.Default.cls_get_short_name(), 'default')
        self.assertEqual(Foo.Bar.cls_get_short_name(), 'bar')
        self.assertEqual(Baz.Qux.cls_get_short_name(), 'qux')
        self.assertEqual(Baz.Default.cls_get_short_name(), 'default')

    def test_sat_columns(self):
        Foo.cls_init()
        cols = Foo.Default.cls_get_columns()
        col1 = cols[0]
        self.assertEqual(col1.name, 'test1')
        self.assertEqual(col1.table.name, 'foo_sat')
        col2 = cols[1]
        self.assertEqual(col2.name, 'test1a')
        cols = Baz.Default.cls_get_columns()
        col1 = cols[0]
        self.assertEqual(col1.name, 'test3')




if __name__ == '__main__':
    unittest.main()
