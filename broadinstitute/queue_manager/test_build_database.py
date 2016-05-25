import build_database
import unittest
import logging
import setup_logger


logger = logging.getLogger(setup_logger.LOGGER_NAME)

queue_manager_config = "example_queue_manager.cfg"


class TestBuildDatabase(unittest.TestCase):
    def test_build(self):
        conn = build_database.build(":memory:", queue_manager_config)
        cursor = conn.cursor()

        cursor.execute("select count(*) from queue_type")
        r = [x for (x,) in cursor][0]
        assert r == 0, r

        cursor.execute("select count(*) from queue")
        r = [x for (x,) in cursor][0]
        assert r == 0, r

        cursor.execute("insert into queue_type(name) values ('my_fake_queue_type')")
        with self.assertRaises(Exception) as context:
            cursor.execute("insert into queue_type(name) values ('my_fake_queue_type')")
        assert context.exception is not None
        logger.debug("context.exception:  {}".format(context.exception))

        conn.close()

    def test_script_methods(self):
        methods = [build_database.insert_initial_espresso_prism_values,
            build_database.insert_initial_psp_values]

        for m in methods:
            logger.debug("m:  {}".format(m))

            conn = build_database.build(":memory:", queue_manager_config)

            precursor = conn.cursor()
            precursor.execute("select count(*) from queue_type")
            r = [x for (x,) in precursor][0]
            assert r == 0, r

            m(conn, queue_manager_config)
            conn.commit()

            cursor = conn.cursor()
            cursor.execute("select count(*) from queue_type")
            r = [x for (x,) in cursor][0]
            assert r > 0, r

            conn.close()


if __name__ == "__main__":
    setup_logger.setup(verbose=True)
    unittest.main()
