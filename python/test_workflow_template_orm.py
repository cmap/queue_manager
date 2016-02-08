import logging
import setup_logger
import unittest
import workflow_template_orm as wto
import build_database


logger = logging.getLogger(setup_logger.LOGGER_NAME)

conn = None


class TestWorkflowTemplateOrm(unittest.TestCase):
    def test___init__(self):
        my_wto = wto.WorkflowTemplateOrm()
        assert hasattr(my_wto, "id")
        assert hasattr(my_wto, "name")
        assert hasattr(my_wto, "queue_type_pairs")
        assert my_wto.queue_type_pairs is not None
        assert len(my_wto.queue_type_pairs) == 0, len(my_wto.queue_type_pairs)

    def test__build_from_cursor_query(self):
        cursor = [(1, "fake workflow template", 2, "fake prev queue type item", 3, "fake next queue type item"),
            (1, "this should be ignored", 5, "another prev queue type item", 7, "my next queue type item"),
            (11, "another workflow template", 13, "third prev queue type item", 17, "third next queue type item"),
            (11, "another thing to ignore", 19, "fourth prev qti", 23, "fourth next qti")]

        r = wto._build_from_cursor_query(cursor)
        assert r
        assert len(r) == 2, len(r)
        for (i, my_wto) in enumerate(r):
            logger.debug("my_wto:  {}".format(my_wto))
            assert len(my_wto.queue_type_pairs) == 2, len(my_wto.queue_type_pairs)

            expected_index = i*2
            assert my_wto.id == cursor[expected_index][0], my_wto.id
            assert my_wto.name == cursor[expected_index][1], my_wto.name

            for qt_index in range(2):
                qt_expected_index = expected_index + qt_index

                pqt = my_wto.queue_type_pairs[qt_index][0]
                assert pqt.id == cursor[qt_expected_index][2], pqt.id
                assert pqt.name == cursor[qt_expected_index][3], pqt.name

                nqt = my_wto.queue_type_pairs[qt_index][1]
                assert nqt.id == cursor[qt_expected_index][4], nqt.id
                assert nqt.name == cursor[qt_expected_index][5], nqt.name

    def test_get_by_id(self):
        cursor = conn.cursor()
        cursor.execute("select id from workflow_template limit 0,1")
        test_id = [x for (x,) in cursor][0]
        logger.debug("test_id:  {}".format(test_id))

        r = wto.get_by_id(cursor, test_id)
        assert r is not None
        logger.debug("r:  {}".format(r))
        assert len(r) == 1, len(r)
        r = r[0]
        assert r.id == test_id, r.id
        assert r.name is not None
        assert len(r.queue_type_pairs) > 0
        qtp = r.queue_type_pairs[0]
        assert len(qtp) == 2, len(qtp)
        assert qtp[0].id is not None
        assert qtp[0].name is not None
        assert qtp[1].id is not None
        assert qtp[1].name is not None

    def test_get_all(self):
        cursor = conn.cursor()
        cursor.execute("select count(*) from workflow_template")
        expected_count = [x for (x,) in cursor][0]
        logger.debug("expected_count:  {}".format(expected_count))

        r = wto.get_all(cursor)
        assert r is not None
        logger.debug("r:  {}".format(r))
        assert len(r) == expected_count, len(r)

if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    conn = build_database.build(":memory:")
    build_database.insert_initial_prism_values(conn)
    conn.commit()

    unittest.main()

    conn.close()
