import unittest
import workflow_orm as wo
import logging


class TestWorkflowOrm(unittest.TestCase):
    def test___init__(self):
        my_wo = wo.WorkflowOrm()
        assert hasattr(my_wo, "id")
        assert hasattr(my_wo, "plate_id")

    def test_delete(self):
        my_wo = wo.WorkflowOrm()
        my_wo.delete(None)

    def test_create(self):
        my_wo = wo.WorkflowOrm()
        my_wo.create(None)

    def test_get_by_plate_id_prev_queue_type_id(self):
        r = wo.get_by_plate_id_prev_queue_type_id(None, None, None)


if __name__ == "__main__":
    unittest.main()
