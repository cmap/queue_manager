import unittest
import workflow_template_orm as wto


class TestWorkflowTemplateOrm(unittest.TestCase):
    def test___init__(self):
        my_wto = wto.WorkflowTemplateOrm()
        assert hasattr(my_wto, "id")
        assert hasattr(my_wto, "name")
        assert hasattr(my_wto, "queue_type_pairs")
        assert my_wto.queue_type_pairs is not None
        assert len(my_wto.queue_type_pairs) == 0, len(my_wto.queue_type_pairs)


    def test_get_by_id(self):
        r = wto.get_by_id(None, None)


if __name__ == "__main__":
    unittest.main()
