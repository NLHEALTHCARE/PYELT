import unittest
import os


class TestCase_Datatransfer(unittest.TestCase):
    pass

    def get_or_create_datatransfer_path(self):
        path = pyelt_config['datatransfer_path']

        if not path:
            from main import get_root_path
            path = get_root_path() + '/data/transfer/'
        path += '/' + self.name
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    #delete /tmp/datatranfser
        if self.source_db:
            path = self.source_db.get_or_create_datatransfer_path()
            import shutil
            shutil.rmtree(path)




    def test_temporary_datatransfer_deleted(self):
        pass




if __name__ == '__main__':
    unittest.main()
