

class QueueOrm(object):
    def __init__(self, id=None, plate_id=None, datetime_added=None,
        queue_type_id=None, queue_type_name=None):

        self.id = id
        self.plate_id = plate_id
        self.datetime_added = datetime_added
        self.queue_type_id = queue_type_id
        self.queue_type_name = queue_type_name

    def delete(self, cursor):
        pass


def get_by_plate_id_queue_type_id(cursor, plate_id, queue_type_id):
    pass
