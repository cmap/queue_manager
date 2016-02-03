import build_database
import sqlite3
import unittest


class TestBuildDatabase(unittest.TestCase):
    def test_build(self):
        conn = build_database.build(":memory:")
        cursor = conn.cursor()

        cursor.execute("select count(*) from queue_type")
        r = [x for (x,) in cursor][0]
        assert r == 0, r

        cursor.execute("select count(*) from queue")
        r = [x for (x,) in cursor][0]
        assert r == 0, r
        
        conn.close()


if __name__ == "__main__":
    unittest.main()
