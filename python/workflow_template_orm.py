
class WorkflowTemplateOrm(object):
    def __init__(self, id=None, name=None, queue_type_pairs=[]):
        self.id = id
        self.name = name
        self.queue_type_pairs = queue_type_pairs


def get_by_id(cursor, workflow_template_id):
    pass
