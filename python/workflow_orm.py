
class WorkflowOrm(object):
    def __init__(self, id=None, plate_id=None, prev_queue_type_id=None,
        prev_queue_type_name=None, next_queue_type_id=None, next_queue_type_name=None):

        self.id = id
        self.plate_id = plate_id
        self.prev_queue_type_id = prev_queue_type_id
        self.prev_queue_type_name = prev_queue_type_name
        self.next_queue_type_id = next_queue_type_id
        self.next_queue_type_name = next_queue_type_name

    def delete(self, cursor):
        pass

    def create(self, cursor):
        pass

def get_by_plate_id_prev_queue_type_id(cursor, plate_id, prev_queue_type_id):
    pass
