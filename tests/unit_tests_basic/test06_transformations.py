import unittest

from pyelt.mappings.transformations import FieldTransformation


class TestCase_Transformations(unittest.TestCase):
    def test_something(self):
        t = FieldTransformation()
        t.field_name = 'id'
        t.new_step('lower({fld})')
        t.new_step("concat({fld}, '01')")
        t.new_step("concat2({fld}, '02')", )
        sql = t.get_sql()
        self.assertEqual("concat2(concat(lower(id), '01'), '02')", sql)

        t.field_name = 'naam'
        sql = t.get_sql()
        self.assertEqual("concat2(concat(lower(naam), '01'), '02')", sql)

        t = FieldTransformation()
        t.field_name = 'test'
        t.new_step("initcap(lower({fld}::text) || '01')")
        sql = t.get_sql()
        self.assertEqual("initcap(lower(test::text) || '01')", sql)

        t = FieldTransformation()
        t.field_name = 'id'
        t.new_step('lower({fld})')
        t.new_step("concat({step1}, '01')")
        t.new_step("concat2({step2}, '02')", )
        sql = t.get_sql()
        self.assertEqual("concat2(concat(lower(id), '01'), '02')", sql)


if __name__ == '__main__':
    unittest.main()
