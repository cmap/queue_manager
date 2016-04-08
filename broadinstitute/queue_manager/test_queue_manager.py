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

    def test_report_completion(self):
        conn = queue_manager.open_database_connection(cfg_path)
        cursor = conn.cursor()

        cursor.execute("insert into queue (plate_id, queue_type_id) values ('1', 1)")
        cursor.execute("insert into workflow (plate_id, prev_queue_type_id, next_queue_type_id) values ('1', 1, 2)")

        conn.commit()
        conn.close()

        queue_manager.report_completion("1", 1, queue_manager_config_path=cfg_path)

        conn = queue_manager.open_database_connection(cfg_path)
        cursor = conn.cursor()

        cursor.execute("select queue_type_id from queue where plate_id='1'")
        r = [x for (x,) in cursor]
        assert len(r) == 1, len(r)
        assert r[0] == 2, r[0]

        cursor.execute("select count(*) from workflow where plate_id='1'")
        r = [x for (x,) in cursor][0]
        assert r == 0, r

    def test_report_completion_no_queue_entry(self):
        queue_manager.report_completion("1", 1, queue_manager_config_path=cfg_path)


if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    cp = ConfigParser.RawConfigParser()
    cp.read(cfg_path)
    db_path = cp.get("Database", "sqlite3_file_path")

    if os.path.exists(db_path):
        os.remove(db_path)

    conn = build_database.build(db_path)
    build_database.insert_initial_espresso_prism_values(conn)
    conn.commit()

    unittest.main()
