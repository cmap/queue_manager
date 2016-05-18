import unittest
import build_database
import logging
import setup_logger
import queue_type_orm as qto


logger = logging.getLogger(setup_logger.LOGGER_NAME)

conn = None
cursor = None


class TestQueueTypeOrm(unittest.TestCase):
    def test_get_by_name(self):
        cursor.execute("insert into queue_type(name) values ('my_fake_queue_type')")

        r = qto.get_by_name(cursor, "my_fake_queue_type")
        assert r is not None
        logger.debug("r:  {}".format(r))

        r = qto.get_by_name(cursor, "not going to find this")
        assert r is None

        conn.rollback()

    def test_get_all(self):
        cursor.execute("insert into queue_type(name) values ('my_fake_queue_type')")
        cursor.execute("insert into queue_type(name) values ('my_fake_queue_type2')")

        r = qto.get_all(cursor)
        assert len(r) == 2, len(r)
        logger.debug("r:  {}".format(r))

        conn.rollback()

if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    conn = build_database.build(":memory:", "queue_manager.cfg")
    cursor = conn.cursor()

    unittest.main()

    cursor.close()
    conn.close()