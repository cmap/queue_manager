import unittest
import queue_orm as qo
import build_database
import logging
import setup_logger


logger = logging.getLogger(setup_logger.LOGGER_NAME)

queue_manager_config_path = "example_queue_manager.cfg"

default_queue_type_id = 2
default_queue_type_name = "fake queue type"

def _build_conn():
    conn = build_database.build(":memory:", queue_manager_config_path)

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

    def test_reset_is_being_processed(self):
        conn = _build_conn()
        cursor = conn.cursor()

        my_qo = qo.QueueOrm(plate_id="1", queue_type_id=default_queue_type_id, is_being_processed=1)
        my_qo.create(cursor)
        logger.debug("my_qo:  {}".format(my_qo))

        cursor.execute("select is_being_processed from queue where id = ?", (my_qo.id,))
        c = cursor.next()[0]
        logger.debug("c:  {}".format(c))
        assert 1 == c, c

        my_qo.reset_is_being_processed(cursor)

        cursor.execute("select is_being_processed from queue where id = ?", (my_qo.id,))
        r = cursor.next()[0]
        logger.debug("r:  {}".format(r))
        assert 0 == r, r

        my_qo.id = None
        with self.assertRaises(Exception) as context:
            my_qo.reset_is_being_processed(cursor)
        assert context.exception
        logger.debug("context.exception:  {}".format(context.exception))
        assert "reset_is_being_processed cannot work when id is None" in str(context.exception)

        cursor.close()
        conn.close()

    def test_delete(self):
        conn = _build_conn()
        cursor = conn.cursor()

        my_qo = qo.QueueOrm(plate_id=1, queue_type_id=default_queue_type_id)
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

    def test_update(self):
        conn = _build_conn()
        cursor = conn.cursor()

        my_qo = qo.QueueOrm(plate_id=1, queue_type_id=default_queue_type_id, is_being_processed=0)
        my_qo.create(cursor)
        my_qo = qo.get_by_plate_id_queue_type_id(cursor, 1, default_queue_type_id)

        my_qo.priority = 3
        my_qo.plate_id = 5
        my_qo.is_being_processed = 1

        my_qo.update(cursor)

        r = qo.get_by_plate_id_queue_type_id(cursor, 5, default_queue_type_id)
        assert r is not None
        logger.debug("r:  {}".format(r))
        assert r.priority == my_qo.priority, r.priority
        assert r.is_being_processed == my_qo.is_being_processed, r.is_being_processed

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

    def test_get_by_plate_id(self):
        conn = _build_conn()
        cursor = conn.cursor()

        r = qo.get_by_plate_id(cursor, "fake plate id")
        assert len(r) == 0, len(r)

        my_qo = qo.QueueOrm(plate_id=11, queue_type_id=default_queue_type_id)
        my_qo.create(cursor)

        r = qo.get_by_plate_id(cursor, 11)
        assert len(r) == 1, len(r)
        r = r[0]
        logger.debug("r:  {}".format(r))
        assert r.plate_id == "11", r.plate_id
        assert r.queue_type_id == default_queue_type_id

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

    def test_delete_by_plate_ids(self):
        conn = _build_conn()
        cursor = conn.cursor()
        for i in range(5):
            cursor.execute("insert into queue (plate_id, queue_type_id) values (?, ?)", (str(i),
                default_queue_type_id))

        cursor.execute("select * from queue")
        r = [x for x in cursor]
        logger.debug("r:  {}".format(r))
        assert len(r) == 5, len(r)

        qo.delete_by_plate_ids(cursor, [1,2,3])

        cursor.execute("select * from queue")
        r = [x for x in cursor]
        logger.debug("r:  {}".format(r))

        assert len(r) == 2, len(r)

        cursor.close()
        conn.close()

    def test_get_by_queue_type_id(self):
        conn = _build_conn()
        cursor = conn.cursor()

        cursor.execute("insert into queue_type (name) values ('my other fake queue type')")
        cursor.execute("select id from queue_type where name = 'my other fake queue type'")
        other_queue_type_id = cursor.fetchone()[0]
        logger.debug("other_queue_type_id:  {}".format(other_queue_type_id))

        N_expected = 3
        for i in xrange(N_expected):
            cursor.execute("insert into queue (queue_type_id, plate_id) values (?, ?)", (default_queue_type_id, i))

        for i in xrange(N_expected+4):
            cursor.execute("insert into queue (queue_type_id, plate_id) values (?, ?)", (other_queue_type_id, i))

        cursor.execute("select count(*) from queue")
        c = cursor.fetchone()[0]
        logger.debug("confirm total added c:  {}".format(c))
        self.assertEqual(2*N_expected+4, c)

        r = qo.get_by_queue_type_id(cursor, default_queue_type_id)
        logger.debug("r:  {}".format(r))
        self.assertEqual(N_expected, len(r))
        r_plate_ids = set([x.plate_id for x in r])
        self.assertEqual(set([str(x) for x in xrange(N_expected)]), r_plate_ids)

        r = qo.get_by_queue_type_id(cursor, other_queue_type_id)
        logger.debug("r:  {}".format(r))
        self.assertEqual(N_expected+4, len(r))
        r_plate_ids = set([x.plate_id for x in r])
        self.assertEqual(set([str(x) for x in xrange(N_expected+4)]), r_plate_ids)

        #query using queue_type_id that does not match anything
        r = qo.get_by_queue_type_id(cursor, default_queue_type_id*other_queue_type_id)
        logger.debug("r:  {}".format(r))
        self.assertEqual(0, len(r))
        
        cursor.close()
        conn.close()

if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    unittest.main()
