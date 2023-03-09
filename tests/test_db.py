import src.python_sqlite as python_sqlite

class TestDb:
    def testdb(self):
        data_obj = {
            "string": "text",
            "int": 35,
            "float": 26.50,
            "bool": True,
        }
        print('Creating DB')
        with python_sqlite.Database("test.db") as db:
            assert not db.table_exists("test")
            db.create_table("test", data_obj)
            assert db.table_exists("test")

        
