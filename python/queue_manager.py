import workflow_orm
import queue_orm
import ConfigParser
import sqlite3
import os.path
import logging
import setup_logger


logger = logging.getLogger(setup_logger.LOGGER_NAME)

default_queue_manager_config_path = os.path.expanduser("~/.queue_manager.cfg")


def report_completion(plate_id, completed_queue_type_id,
    queue_manager_config_path=default_queue_manager_config_path):

    logger.debug("queue_manager_config_path:  {}".format(queue_manager_config_path))

    conn = _open_database_connection(queue_manager_config_path)
    cursor = conn.cursor()

    previous_queue_orm = queue_orm.get_by_plate_id_queue_type_id(cursor, plate_id,
        completed_queue_type_id)
    if previous_queue_orm is not None:
        previous_queue_orm.delete(cursor)

    workflows = workflow_orm.get_by_plate_id_prev_queue_type_id(cursor, plate_id,
        completed_queue_type_id)

    for w in workflows:
        qo = queue_orm.QueueOrm(plate_id=plate_id, queue_type_id=w.next_queue_type_id)
        qo.create(cursor)
        w.delete(cursor)

    conn.commit()
    conn.close()


def _open_database_connection(queue_manager_config_path):
    logger.debug("queue_manager_config_path:  {}".format(queue_manager_config_path))

    cp = ConfigParser.RawConfigParser()
    cp.read(queue_manager_config_path)

    conn = sqlite3.connect(cp.get("Database", "sqlite3_file_path"))

    return conn
