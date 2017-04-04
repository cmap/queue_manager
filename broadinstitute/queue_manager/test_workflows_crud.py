import workflows_crud as wc
import logging
import setup_logger
import unittest
import build_database
import workflow_orm
import workflow_template_orm


logger = logging.getLogger(setup_logger.LOGGER_NAME)

conn = None

queue_manager_config_path = "example_queue_manager.cfg"

class TestWorkflowsCrud(unittest.TestCase):
    def test_create(self):
        cursor = conn.cursor()
        cursor.execute("select count(*) from workflow where plate_id=2")
        r = [x for (x,) in cursor][0]
        assert r == 0, r

        plate_ids = [2,3]
        wc.create(cursor, "L1000 espresso", plate_ids)

        cursor.execute("select count(*) from workflow where plate_id=2")
        r = [x for (x,) in cursor][0]
        assert r > 0, r

        l1000_wto = workflow_template_orm.get_by_name(cursor, "L1000 espresso")
        expected_count = len(l1000_wto.queue_type_pairs)

        for pid in plate_ids:
            found = workflow_orm.get_by_plate_id(cursor, pid)
            assert found is not None
            logger.debug("found:  {}".format(found))
            assert len(found) == expected_count, len(found)

        conn.rollback()
        cursor.close()

    def test_validate_args(self):
        string_args = ["create", "-plate_ids", "2"]
        args = wc.build_parser().parse_args(string_args)
        r = wc.validate_args(args)
        assert r == False

        string_args[1] = "-wtn"
        logger.debug("string_args:  {}".format(string_args))
        args = wc.build_parser().parse_args(string_args)
        r = wc.validate_args(args)
        assert r == False

        string_args.extend(["-plate_ids", "3"])
        logger.debug("string_args:  {}".format(string_args))
        args = wc.build_parser().parse_args(string_args)
        r = wc.validate_args(args)
        assert r == True

    def test_list_templates(self):
        cursor = conn.cursor()
        wc.list_templates(cursor)
        cursor.close()

    def test_delete_by_plate_ids(self):
        cursor = conn.cursor()

        cursor.execute("select count(*) from workflow where plate_id=2")
        c = cursor.fetchone()[0]
        logger.debug("check that there are no workflows already present before running test for plate_id=2 - c:  {}".format(c))

        wc.create(cursor, "L1000 espresso", [2])
        cursor.execute("select count(*) from workflow where plate_id=2")
        c = cursor.fetchone()[0]
        logger.debug("check that there workflows were successfully added already present before running delete for plate_id=2 - c:  {}".format(c))
        self.assertGreater(c, 0)

        wc.delete_by_plate_ids(cursor, [2])
        cursor.execute("select count(*) from workflow where plate_id=2")
        r = cursor.fetchone()[0]
        logger.debug("remaining workflow entries - r:  {}".format(r))
        self.assertEqual(0, r)

        cursor.close()
        conn.rollback()
            
if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    conn = build_database.build(":memory:", queue_manager_config_path)
    build_database.insert_initial_espresso_prism_values(conn, queue_manager_config_path)
    conn.commit()

    unittest.main()

    conn.close()
