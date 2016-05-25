import queue_type_orm
import logging
import setup_logger


logger = logging.getLogger(setup_logger.LOGGER_NAME)

base_query = """select wt.id, wt.name, wtp.prev_queue_type_id, pqt.name, wtp.next_queue_type_id, nqt.name
 from workflow_template wt
 join workflow_template_pair wtp on wtp.workflow_template_id = wt.id
 join queue_type pqt on pqt.id = wtp.prev_queue_type_id
 join queue_type nqt on nqt.id = wtp.next_queue_type_id
 """


class WorkflowTemplateOrm(object):
    def __init__(self, id=None, name=None):
        self.id = id
        self.name = name
        self.queue_type_pairs = []

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        qtp = []
        for x in self.queue_type_pairs:
            qtp.append(str(x[0]) + "-->" + str(x[1]))
        qtp_str = " ".join(qtp)
        return "id:{} name:{} queue_type_pairs:{}".format(self.id, self.name, qtp)


def get_by_id(cursor, workflow_template_id):
    query = base_query + " where wt.id = ?"
    cursor.execute(query, (workflow_template_id,))
    r = _build_from_cursor_query(cursor)
    assert len(r) == 1, len(r)
    return r[0]


def get_all(cursor):
    cursor.execute(base_query)
    return _build_from_cursor_query(cursor)


def get_by_name(cursor, workflow_template_name):
    query = base_query + " where wt.name = ?"
    cursor.execute(query, (workflow_template_name,))
    r = _build_from_cursor_query(cursor)
    assert len(r) == 1, len(r)
    return r[0]


def _build_from_cursor_query(cursor):
    r = {}
    for (wt_id, name, prev_qt_id, prev_qt_name, next_qt_id, next_qt_name) in cursor:
        logger.debug("wt_id:  {}".format(wt_id))
        if wt_id not in r:
            new_wto = WorkflowTemplateOrm(id=wt_id, name=name)
            r[wt_id] = new_wto

        wto = r[wt_id]

        pqt = queue_type_orm.QueueTypeOrm(id=prev_qt_id, name=prev_qt_name)
        nqt = queue_type_orm.QueueTypeOrm(id=next_qt_id, name=next_qt_name)
        wto.queue_type_pairs.append((pqt, nqt))

        logger.debug("wto:  {}".format(wto))

    return r.values()
