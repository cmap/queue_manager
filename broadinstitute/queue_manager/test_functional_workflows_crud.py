import workflows_crud
import logging
import setup_logger
import unittest
import build_database
import os


logger = logging.getLogger(setup_logger.LOGGER_NAME)

queue_manager_config_path = "functional_tests/test_functional_workflows_crud/queue_manager.cfg"

class FunctionalTestWorkflowsCrud(unittest.TestCase):
    def test_create(self):
        #setup database
        config = workflows_crud.read_config(queue_manager_config_path)
        db_file_path = config.get("Database", "sqlite3_file_path")
        if os.path.exists(db_file_path):
            os.remove(db_file_path)
        conn = build_database.build(db_file_path, queue_manager_config_path)
        build_database.insert_initial_psp_values(conn, queue_manager_config_path)
        conn.commit()

        #setup arguments
        string_args = ["-v", "-queue_manager_config_file", queue_manager_config_path,
            "create", "-plate_ids", "1", "-wtn", "P100"]
        args = workflows_crud.build_parser().parse_args(string_args)
        logger.debug("args:  {}".format(args))
        assert workflows_crud.validate_args(args)

        workflows_crud.main(args)

        cursor = conn.cursor()
        cursor.execute("select count(*) from workflow")
        r = [x for (x,) in cursor][0]
        logger.debug("select count(*) from workflow - r:  {}".format(r))
        assert r > 0


if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    unittest.main()
