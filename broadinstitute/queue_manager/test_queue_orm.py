import unittest
import queue_orm as qo
import sqlite3
import sys
import build_database
import logging
import setup_logger


logger = logging.getLogger(setup_logger.LOGGER_NAME)



default_queue_type_id = 2
default_queue_type_name = "fake queue type"

def _build_conn():
    conn = build_database.build(":memory:", "queue_manager.cfg")

    cursor = conn.cursor()
    cursor.execute("insert into queue_type (id,name) values (?, ?)",
        (default_queue_type_id, default_queue_type_name))
    conn.commit()
    cursor.close()

    return conn


class TestQueueOrm(unittest.TestCase):
    def test____init__(self):
        my_qo = qo.QueueOrm()
        assert hasattr(my_qo, "id")
        assert hasattr(my_qo, "plate_id")

    def test_create(self):
        conn = _build_conn()
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

        cursor.close()
        conn.close()

    def test_delete(self):
        conn = _build_conn()
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
        conn.close()

    def test_get_by_plate_id_queue_type_id(self):
        conn = _build_conn()
        cursor = conn.cursor()

        r = qo.get_by_plate_id_queue_type_id(cursor, "fake plate id", default_queue_type_id)
        assert r is None, r
        
        plate_id_db_id_map = {}
        N = 3
        for plate_id in range(11, 11+N):
            my_qo = qo.QueueOrm(plate_id=plate_id, queue_type_id=default_queue_type_id)
            my_qo.create(cursor)
            plate_id_db_id_map[plate_id] = my_qo.id

        for (plate_id, db_id) in plate_id_db_id_map.items():
            r = qo.get_by_plate_id_queue_type_id(cursor, plate_id, default_queue_type_id)
            logger.debug("r:  {}".format(r))
            assert r.id == db_id, r.id

        cursor.close()
        conn.close()

    def test_checkout_top_N_items(self):
        conn = _build_conn()
        cursor = conn.cursor()

        for i in range(5):
            priority = i + 100
            cursor.execute("insert into queue (plate_id, priority, queue_type_id) values (?, ?, ?)", (str(i), priority,
                default_queue_type_id))
        cursor.execute("update queue set is_being_processed=1 where plate_id='1'")

        r = qo.checkout_top_N_items(cursor, default_queue_type_id, 3)
        for r_indiv in r:
            logger.debug("r_indiv:  {}".format(r_indiv))

        assert len(r) == 3
        assert r[0].plate_id == "0", r[0].plate_id
        assert r[1].plate_id == "2", r[1].plate_id
        assert r[2].plate_id == "3", r[2].plate_id

        cursor.execute("select distinct is_being_processed from queue where plate_id in ('0','2','3')")
        r = [x for (x,) in cursor]
        logger.debug("r:  {}".format(r))
        assert len(r) == 1, len(r)
        assert r[0] == 1, r[0]

        cursor.close()
        conn.close()

    def test_get_all(self):
        conn = _build_conn()
        cursor = conn.cursor()

        for i in range(5):
            priority = i + 100
            cursor.execute("insert into queue (plate_id, priority, queue_type_id) values (?, ?, ?)", (str(i), priority,
                default_queue_type_id))
        cursor.execute("update queue set is_being_processed=1 where plate_id='1'")

        r = qo.get_all(cursor)
        for r_indiv in r:
            logger.debug("r_indiv:  {}".format(r_indiv))

        assert len(r) == 5, len(r)
        assert r[0].plate_id == "1", r[0].plate_id
        assert r[0].is_being_processed == True, r[0].is_being_processed
        assert r[1].plate_id == "0", r[1].plate_id
        assert r[2].plate_id == "2", r[2].plate_id

        cursor.close()
        conn.close()

if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    unittest.main()
