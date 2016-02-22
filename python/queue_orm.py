import logging
import setup_logger


logger = logging.getLogger(setup_logger.LOGGER_NAME)


class QueueOrm(object):
    def __init__(self, id=None, plate_id=None, datetime_added=None,
        queue_type_id=None, queue_type_name=None, priority=None,
        is_being_processed=None):

        self.id = id
        self.plate_id = plate_id
        self.datetime_added = datetime_added
        self.queue_type_id = queue_type_id
        self.queue_type_name = queue_type_name
        self.priority = priority
        self.is_being_processed = is_being_processed

    def delete(self, cursor):
        if self.id is not None:
            logger.debug("deleting self.id:  {}".format(self.id))
            cursor.execute("delete from queue where id = ?", (self.id,))
        else:
            raise Exception("QueueOrm delete cannot delete when self.id is None")

    def create(self, cursor):
        if self.plate_id is not None and self.queue_type_id is not None:
            values = [self.plate_id, self.queue_type_id]

            extra_columns = []
            if self.priority is not None:
                extra_columns.append("priority")
                values.append(self.priority)
            if self.is_being_processed is not None:
                extra_columns.append("is_being_processed")
                values.append(self.is_being_processed)

            query = "insert into queue (plate_id, queue_type_id{}) values (?, ?{})"
            ec = ""
            ev = ""
            if len(extra_columns) > 0:
                ec = ", " + ", ".join(extra_columns)
                ev = ", " + ", ".join(["?" for x in range(len(extra_columns))])

            cursor.execute(query.format(ec, ev), values)

            self.id = cursor.lastrowid
        else:
            raise Exception("QueueOrm create cannot create when one of these is None - self.plate_id:  {}  self.queue_type_id:  {}".format(self.plate_id, self.queue_type_id))

    def __str__(self):
        return " ".join(["{}:{}".format(k,v) for (k,v) in self.__dict__.items()])


def get_by_plate_id_queue_type_id(cursor, plate_id, queue_type_id):
    cursor.execute("""select q.id, plate_id, datetime_added, q.queue_type_id, qt.name
from queue q
join queue_type qt on qt.id = q.queue_type_id
where plate_id = ? and q.queue_type_id = ?""", (plate_id, queue_type_id))

    r = [QueueOrm(id=x[0], plate_id=x[1], datetime_added=x[2], queue_type_id=x[3],
        queue_type_name=x[4]) for x in cursor]

    if len(r) == 1:
        return r[0]
    elif len(r) == 0:
        return None
    else:
        raise Exception("queue_orm get_by_plate_id_queue_type_id based on unique constraint in database, expected only 1 or 0 items, found len(r):  {}".format(len(r)))
