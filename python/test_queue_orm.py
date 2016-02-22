import unittest
import queue_orm as qo
import sqlite3
import sys
import build_database
import logging
import setup_logger


logger = logging.getLogger(setup_logger.LOGGER_NAME)

conn = build_database.build(":memory:")

default_queue_type_id = 2
default_queue_type_name = "fake queue type"


class TestQueueOrm(unittest.TestCase):
    def test____init__(self):
        my_qo = qo.QueueOrm()
        assert hasattr(my_qo, "id")
        assert hasattr(my_qo, "plate_id")

    def test_create(self):
        cursor = conn.cursor()

        my_qo = qo.QueueOrm(plate_id="1", queue_type_id=default_queue_type_id)

        my_qo.create(cursor)

        cursor.execute("select id, plate_id, queue_type_id from queue where plate_id=?",
            (my_qo.plate_id,))
        r = [x for x in cursor][0]
        logger.debug("r:  {}".format(r))

        assert r[0] == my_qo.id, r[0]
        assert r[1] == my_qo.plate_id, "r[1]:  {}  my_qo.plate_id:  {}".format(r[1], my_qo.plate_id)
        assert r[2] == my_qo.queue_type_id, r[2]

        my_qo = qo.QueueOrm(plate_id="2", queue_type_id=default_queue_type_id,
            priority=3.3, is_being_processed=True)
        my_qo.create(cursor)

        cursor.execute("select id, plate_id, queue_type_id, priority, is_being_processed from queue where plate_id=?", (my_qo.plate_id,))

        r = [x for x in cursor][0]
        logger.debug("r:  {}".format(r))

        assert r[0] == my_qo.id, r[0]
        assert r[1] == my_qo.plate_id, r[1]
        assert r[2] == my_qo.queue_type_id, r[2]
        assert r[3] == my_qo.priority, r[3]
        assert r[4] == my_qo.is_being_processed, r[4]

        conn.rollback()

    def test_delete(self):
        cursor = conn.cursor()

        my_qo = qo.QueueOrm(plate_id=1, queue_type_id=2)
        my_qo.create(cursor)
        my_qo.delete(cursor)

        cursor.execute("select count(*) from queue where id = ?", (my_qo.id,))
        r = [x for (x,) in cursor][0]
        assert r == 0, r

        conn.rollback()
        cursor.close()
        cursor = conn.cursor()

        with self.assertRaises(Exception) as context:
            my_qo.id = None
            my_qo.delete(cursor)
        assert context.exception
        logger.debug("context.exception:  {}".format(context.exception))
        assert "cannot delete when self.id is None" in str(context.exception)

        cursor.close()

    def test_get_by_plate_id_queue_type_id(self):
        cursor = conn.cursor()

        plate_id_db_id_map = {}
        N = 3
        for plate_id in range(11, 11+N):
            my_qo = qo.QueueOrm(plate_id=plate_id, queue_type_id=default_queue_type_id)
            my_qo.create(cursor)
            plate_id_db_id_map[plate_id] = my_qo.id

        for (plate_id, db_id) in plate_id_db_id_map.items():
            r = qo.get_by_plate_id_queue_type_id(cursor, plate_id, default_queue_type_id)
            assert len(r) == 1, len(r)
            r = r[0]
            logger.debug("r:  {}".format(r))
            assert r.id == db_id, r.id

        conn.rollback()
        cursor.close()


if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    cursor = conn.cursor()
    cursor.execute("insert into queue_type (id,name) values (?, ?)",
        (default_queue_type_id, default_queue_type_name))
    conn.commit()
    cursor.close()

    unittest.main()

    conn.close()
