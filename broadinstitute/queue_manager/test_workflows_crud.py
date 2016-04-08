import workflows_crud as wc
import logging
import setup_logger
import unittest
import build_database
import workflow_orm
import workflow_template_orm


logger = logging.getLogger(setup_logger.LOGGER_NAME)

conn = None


class TestWorkflowsCrud(unittest.TestCase):
    def test_create(self):
        precursor = conn.cursor()
        precursor.execute("select count(*) from workflow where plate_id=2")
        r = [x for (x,) in precursor][0]
        assert r == 0, r
        precursor.close()

        plate_ids = [2,3]
        wc.create(conn, False, "L1000 espresso", plate_ids)

        cursor = conn.cursor()
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

if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    conn = build_database.build(":memory:")
    build_database.insert_initial_espresso_prism_values(conn)
    conn.commit()

    unittest.main()

    conn.close()
