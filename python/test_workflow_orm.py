import workflow_orm as wo
import logging
import unittest
import sqlite3
import sys
import build_database
import logging
import setup_logger


logger = logging.getLogger(setup_logger.LOGGER_NAME)

conn = build_database.build(":memory:")

default_queue_type_map = {2:"fake queue type", 3:"another fqt", 5:"third fqt"}


class TestWorkflowOrm(unittest.TestCase):
    def test___init__(self):
        my_wo = wo.WorkflowOrm(id=3)
        assert hasattr(my_wo, "id")
        assert hasattr(my_wo, "plate_id")
        assert my_wo.id == 3, my_wo.id

    def test_delete(self):
        cursor = conn.cursor()
        my_wo = wo.WorkflowOrm(plate_id=2, prev_queue_type_id=3,next_queue_type_id=5)
        my_wo.create(cursor)

        cursor.execute("select count(*) from workflow where id = ?", (my_wo.id,))
        r = [x for (x,) in cursor][0]
        assert r == 1, r

        my_wo.plate_id = None
        my_wo.prev_queue_type_id = None
        my_wo.next_queue_type_id = None
        my_wo.delete(cursor)
        cursor.execute("select count(*) from workflow where id = ?", (my_wo.id,))
        r = [x for (x,) in cursor][0]
        assert r == 0, r

        my_wo.id = None
        with self.assertRaises(Exception) as context:
            my_wo.delete(cursor)
        assert context.exception
        logger.debug("context.exception:  {}".format(context.exception))
        assert "WorkflowOrm delete cannot delete when self.id is None" in str(context.exception)

        conn.rollback()
        cursor.close()

    def test_create(self):
        cursor = conn.cursor()
        my_wo = wo.WorkflowOrm(plate_id=2, prev_queue_type_id=3,next_queue_type_id=5)
        my_wo.create(cursor)

        assert my_wo.id is not None

        cursor.execute("select count(*) from workflow where plate_id=2 and prev_queue_type_id=3 and next_queue_type_id=5")
        r = [x for (x,) in cursor][0]
        assert r == 1, r

        my_wo.id = None
        my_wo.plate_id = None
        with self.assertRaises(Exception) as context:
            my_wo.create(cursor)
        assert context.exception
        logger.debug("context.exception:  {}".format(context.exception))
        assert "WorkflowOrm create cannot create when one of these is None" in str(context.exception)

        conn.rollback()
        cursor.close()

    def test_get_by_plate_id_prev_queue_type_id(self):
        cursor = conn.cursor()

        plate_id_db_id_map = {}
        N = 3
        for plate_id in range(11, 11+N):
            my_wo = wo.WorkflowOrm(plate_id=plate_id, prev_queue_type_id=2,
                next_queue_type_id=3)
            my_wo.create(cursor)
            plate_id_db_id_map[plate_id] = my_wo.id

        cursor.execute("""select * from workflow w
            join queue_type qtp on qtp.id = w.prev_queue_type_id
            join queue_type qtn on qtn.id = w.next_queue_type_id
            """)
        r = [x for x in cursor]
        logger.debug("r:  {}".format(r))

        for (plate_id, db_id) in plate_id_db_id_map.items():
            r = wo.get_by_plate_id_prev_queue_type_id(cursor, plate_id, 2)
            assert len(r) == 1, len(r)
            r = r[0]
            logger.debug("r:  {}".format(r))
            assert r.id == db_id, r.id
            assert r.next_queue_type_id == 3, r.next_queue_type_id
            assert r.prev_queue_type_name == "fake queue type", r.prev_queue_type_name
            assert r.next_queue_type_name == "another fqt", r.next_queue_type_name

        conn.rollback()
        cursor.close()

    def test__build_from_cursor_query(self):
        cursor = [range(6)]
        r = wo._build_from_cursor_query(cursor)
        assert r is not None
        logger.debug("r:  {}".format(r))
        assert len(r) == 1, len(r)
        r = r[0]
        assert r.id == 0, r.id
        assert r.plate_id == 1, r.plate_id
        assert r.prev_queue_type_id == 2, r.prev_queue_type_id
        assert r.prev_queue_type_name == 3, r.prev_queue_type_name
        assert r.next_queue_type_id == 4, r.next_queue_type_id
        assert r.next_queue_type_name == 5, r.next_queue_type_name

    def test_get_by_plate_id(self):
        cursor = conn.cursor()
        my_wo = wo.WorkflowOrm(plate_id=1, prev_queue_type_id=2,
            next_queue_type_id=3)
        my_wo.create(cursor)
        my_wo = wo.WorkflowOrm(plate_id=1, prev_queue_type_id=3,
            next_queue_type_id=5)
        my_wo.create(cursor)

        cursor.execute("select count(*) from workflow where plate_id=1")
        r = [x for (x,) in cursor][0]
        logger.debug("count of prepared workflow entries r:  {}".format(r))
        assert r == 2, r

        r = wo.get_by_plate_id(cursor, 1)
        assert r is not None
        logger.debug("r:  {}".format(r))
        assert len(r) == 2, len(r)

        conn.rollback()
        cursor.close()

if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    cursor = conn.cursor()
    for (my_id,name) in default_queue_type_map.items():
        cursor.execute("insert into queue_type (id,name) values (?, ?)",
            (my_id, name))
    conn.commit()
    cursor.close()

    unittest.main()

    conn.close()
