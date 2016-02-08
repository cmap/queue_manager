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

    def test_insert_initial_prism_values(self):
        conn = build_database.build(":memory:")

        precursor = conn.cursor()
        precursor.execute("select count(*) from queue_type")
        r = [x for (x,) in precursor][0]
        assert r == 0, r

        build_database.insert_initial_prism_values(conn)
        conn.commit()

        cursor = conn.cursor()
        cursor.execute("select count(*) from queue_type")
        r = [x for (x,) in cursor][0]
        assert r > 0, r

        conn.close()


if __name__ == "__main__":
    unittest.main()
