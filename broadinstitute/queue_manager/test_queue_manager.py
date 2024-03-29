import unittest
import logging
import setup_logger
import build_database
import ConfigParser
import os
import queue_manager


logger = logging.getLogger(setup_logger.LOGGER_NAME)

cfg_path = "functional_tests/test_queue_manager/queue_manager.cfg"


class TestQueueManager(unittest.TestCase):
    @classmethod
    def tearDownClass(cls):
        os.remove("functional_tests/test_queue_manager/test_queue_manager.sqlite3")

    def test_open_database_connection(self):
        test_conn = queue_manager.open_database_connection(cfg_path)
        assert test_conn is not None
        cursor = test_conn.cursor();

        cursor.execute("select count(*) from workflow_template_pair")
        r = [x for x in cursor][0]
        assert r > 0

        cursor.execute("select count(*) from queue_type")
        r = [x for x in cursor][0]
        assert r > 0

        cursor.execute("select count(*) from workflow_template")
        r = [x for x in cursor][0]
        assert r > 0

        test_conn.close()

    def test_report_completion_using_config(self):
        conn = queue_manager.open_database_connection(cfg_path)
        cursor = conn.cursor()

        cursor.execute("insert into queue (plate_id, queue_type_id) values ('1', 1)")
        cursor.execute("insert into workflow (plate_id, prev_queue_type_id, next_queue_type_id) values ('1', 1, 2)")

        conn.commit()
        conn.close()

        queue_manager.report_completion_using_config(["1"], 1, cfg_path)

        conn = queue_manager.open_database_connection(cfg_path)
        cursor = conn.cursor()

        cursor.execute("select queue_type_id from queue where plate_id='1'")
        r = [x for (x,) in cursor]
        assert len(r) == 1, len(r)
        assert r[0] == 2, r[0]

        cursor.execute("select count(*) from workflow where plate_id='1'")
        r = [x for (x,) in cursor][0]
        assert r == 0, r

    def test_report_completion_using_config_no_queue_entry(self):
        queue_manager.report_completion_using_config("1", 1, cfg_path)

    def test_report_completion(self):
        conn = queue_manager.open_database_connection(cfg_path)
        cursor = conn.cursor()

        cursor.execute("delete from queue")
        cursor.execute("delete from workflow")
        cursor.execute("insert into queue (plate_id, queue_type_id) values ('1', 1)")
        cursor.execute("insert into workflow (plate_id, prev_queue_type_id, next_queue_type_id) values ('1', 1, 2)")
        cursor.execute("insert into queue (plate_id, queue_type_id) values ('2', 1)")
        cursor.execute("insert into workflow (plate_id, prev_queue_type_id, next_queue_type_id) values ('2', 1, 2)")

        queue_manager.report_completion(cursor, ["1", "2"], 1)

        cursor.execute("select queue_type_id from queue where plate_id='1'")
        r = [x for (x,) in cursor]
        assert len(r) == 1, len(r)
        assert r[0] == 2, r[0]

        cursor.execute("select count(*) from workflow where plate_id='1'")
        r = [x for (x,) in cursor][0]
        assert r == 0, r

        conn.rollback()
        conn.close()
        conn.close()


if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    cp = ConfigParser.RawConfigParser()
    cp.read(cfg_path)
    db_path = cp.get("Database", "sqlite3_file_path")

    if os.path.exists(db_path):
        os.remove(db_path)

    conn = build_database.build(db_path, cfg_path)
    build_database.insert_initial_espresso_prism_values(conn, cfg_path)
    conn.commit()

    unittest.main()

    conn.close()
