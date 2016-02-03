import logging
import setup_logger


logger = logging.getLogger(setup_logger.LOGGER_NAME)


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
        if self.id is not None:
            logger.debug("deleting self.id:  {}".format(self.id))
            cursor.execute("delete from workflow where id = ?", (self.id,))
        else:
            raise Exception("WorkflowOrm delete cannot delete when self.id is None")


    def create(self, cursor):
        if self.plate_id is not None and self.prev_queue_type_id is not None and self.next_queue_type_id is not None:
            cursor.execute("""insert into workflow (plate_id, prev_queue_type_id, next_queue_type_id) values (?, ?, ?)""",
                (self.plate_id, self.prev_queue_type_id, self.next_queue_type_id))

            self.id = cursor.lastrowid
        else:
            raise Exception("WorkflowOrm create cannot create when one of these is None - self.plate_id:  {}  self.prev_queue_type_id:  {}  self.next_queue_type_id:  {}".format(self.plate_id, self.prev_queue_type_id, self.next_queue_type_id))

    def __str__(self):
        return " ".join(["{}:{}".format(k,v) for (k,v) in self.__dict__.items()])


def get_by_plate_id_prev_queue_type_id(cursor, plate_id, prev_queue_type_id):
    logger.debug("plate_id:  {}  prev_queue_type_id:  {}".format(plate_id, prev_queue_type_id))

    cursor.execute("""select w.id, plate_id, prev_queue_type_id, qtp.name, next_queue_type_id, qtn.name
 from workflow w
 join queue_type qtp on qtp.id = w.prev_queue_type_id
 join queue_type qtn on qtn.id = w.next_queue_type_id
 where plate_id = ? and prev_queue_type_id = ?""", (plate_id, prev_queue_type_id))

    r = [WorkflowOrm(id=x[0], plate_id=x[1], prev_queue_type_id=x[2],
        prev_queue_type_name=x[3], next_queue_type_id=x[4], next_queue_type_name=x[5])
        for x in cursor]

    return r
