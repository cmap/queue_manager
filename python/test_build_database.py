import build_database
import sqlite3
import unittest
import logging
import setup_logger


logger = logging.getLogger(setup_logger.LOGGER_NAME)


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

    def test_script_methods(self):
        methods = [build_database.insert_initial_espresso_prism_values,
            build_database.insert_initial_psp_values]

        for m in methods:
            logger.debug("m:  {}".format(m))

            conn = build_database.build(":memory:")

            precursor = conn.cursor()
            precursor.execute("select count(*) from queue_type")
            r = [x for (x,) in precursor][0]
            assert r == 0, r

            m(conn)
            conn.commit()

            cursor = conn.cursor()
            cursor.execute("select count(*) from queue_type")
            r = [x for (x,) in cursor][0]
            assert r > 0, r

            conn.close()


if __name__ == "__main__":
    setup_logger.setup(verbose=True)
    unittest.main()
