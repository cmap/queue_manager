import logging
import setup_logger
import sql_query_utils


logger = logging.getLogger(setup_logger.LOGGER_NAME)

_base_query = """select q.id, q.plate_id, q.datetime_added, q.queue_type_id, qt.name, q.priority, q.is_being_processed
from queue q
join queue_type qt on qt.id = q.queue_type_id"""


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

    def update(self, cursor):
        if self.id is not None and self.plate_id is not None and self.queue_type_id is not None:
            cursor.execute("update queue set plate_id=?, datetime_added=?, queue_type_id=?, priority=?, is_being_processed=? where id=?",
                           (self.plate_id, self.datetime_added, self.queue_type_id, self.priority, self.is_being_processed, self.id))

    def reset_is_being_processed(self, cursor):
        if self.id is not None:
            cursor.execute("update queue set is_being_processed = 0 where id = ?", (self.id,))
        else:
            raise Exception("QueueOrm reset_is_being_processed cannot work when id is None - self:  {}".format(self))

    def __str__(self):
        return " ".join(["{}:{}".format(k,v) for (k,v) in self.__dict__.items()])


def _build_queue_orm_from_queury_result(cursor):
    r = [QueueOrm(id=x[0], plate_id=x[1], datetime_added=x[2], queue_type_id=x[3], queue_type_name=x[4], priority=x[5],
                  is_being_processed=x[6]) for x in cursor]
    return r


def get_by_plate_id_queue_type_id(cursor, plate_id, queue_type_id):
    cursor.execute(_base_query + " where q.plate_id = ? and q.queue_type_id = ?", (plate_id, queue_type_id))

    r = _build_queue_orm_from_queury_result(cursor)

    if len(r) == 1:
        return r[0]
    elif len(r) == 0:
        return None
    else:
        raise Exception("queue_orm get_by_plate_id_queue_type_id based on unique constraint in database, expected only 1 or 0 items, found len(r):  {}".format(len(r)))

def checkout_top_N_items(cursor, queue_type_id, N):
    cursor.execute(_base_query + " where q.is_being_processed=0 and q.queue_type_id = ? order by q.priority limit 0,?",
                   (queue_type_id, N))

    r = _build_queue_orm_from_queury_result(cursor)

    for qo in r:
        cursor.execute("update queue set is_being_processed = 1 where id = ?", (qo.id,))

    return r

def get_all(cursor):
    cursor.execute(_base_query + " order by q.is_being_processed desc, qt.name, q.priority")

    return _build_queue_orm_from_queury_result(cursor)

def delete_by_plate_ids(cursor, plate_ids):
    plate_ids_in_clause = sql_query_utils.build_in_clause(plate_ids)
    query_str = "delete from queue where plate_id in ({})".format(plate_ids_in_clause)
    logger.debug("query_str:  {}".format(query_str))
    cursor.execute(query_str)

def get_by_plate_id(cursor, plate_id):
    cursor.execute(_base_query + " where q.plate_id = ?", (plate_id,))
    return _build_queue_orm_from_queury_result(cursor)
