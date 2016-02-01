import unittest
import queue_orm as qo


class TestQueueOrm(unittest.TestCase):
    def test____init__(self):
        my_qo = qo.QueueOrm()
        assert hasattr(my_qo, "id")
        assert hasattr(my_qo, "plate_id")

    def test_delete(self):
        my_qo = qo.QueueOrm()
        my_qo.delete(None)

    def test_get_by_plate_id_queue_type_id(self):
        qo.get_by_plate_id_queue_type_id(None, None, None)

if __name__ == "__main__":
    unittest.main()
