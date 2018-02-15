import logging
import setup_logger
import unittest
import build_database
import os
import ConfigParser


logger = logging.getLogger(setup_logger.LOGGER_NAME)

queue_manager_config_path = "functional_tests/test_functional_database/queue_manager.cfg"


class FunctionalTestDatabase(unittest.TestCase):
    @classmethod
    def tearDownClass(cls):
        os.remove("functional_tests/test_functional_database/test_functional_database.sqlite3")

    @staticmethod
    def setup_database():
        config = ConfigParser.RawConfigParser()
        config.read(queue_manager_config_path)
        db_file_path = config.get("Database", "sqlite3_file_path")
        logger.debug("db_file_path:  {}".format(db_file_path))

        if os.path.exists(db_file_path):
            os.remove(db_file_path)

        conn = build_database.build(db_file_path, queue_manager_config_path)

        build_database.insert_initial_espresso_prism_values(conn, queue_manager_config_path)

        return conn

    def test_workflow_template_pair_table(self):
        conn = FunctionalTestDatabase.setup_database()

        cursor = conn.cursor()

        cursor.execute("select count(distinct workflow_template_id) from workflow_template_pair")
        r = [x for (x,) in cursor][0]
        assert r == 2, r

        cursor.execute("select id, name from queue_type")
        queue_type_dict = {}
        for (id, name) in cursor:
            queue_type_dict[name] = id

        cursor.execute("select prev_queue_type_id from workflow_template_pair where next_queue_type_id = ?",
                       (queue_type_dict["compare plates"],))
        r = [x for (x,) in cursor]
        assert len(r) == 1, len(r)
        assert r[0] == queue_type_dict["brew"], (r[0], queue_type_dict)

        cursor.execute("select next_queue_type_id from workflow_template_pair where prev_queue_type_id = ?",
                       (queue_type_dict["compare plates"],))
        r = [x for (x,) in cursor]
        assert len(r) == 1, len(r)
        assert r[0] == queue_type_dict["S3ify"], (r[0], queue_type_dict)

        cursor.close()
        conn.close()

    def test_prism_espresso_update002_insert_initial_values(self):
        conn = FunctionalTestDatabase.setup_database()

        #repeated application of this script does not cause problems
        build_database.insert_initial_espresso_prism_values(conn, queue_manager_config_path)
        build_database.insert_initial_espresso_prism_values(conn, queue_manager_config_path)

        conn.close()

if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    unittest.main()
