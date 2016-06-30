import workflow_orm
import queue_orm
import ConfigParser
import sqlite3
import logging
import setup_logger
import argparse
import sys
import os.path


logger = logging.getLogger(setup_logger.LOGGER_NAME)


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-verbose', '-v', help='Whether to print a bunch of output.',
        action='store_true', default=False)
    parser.add_argument("-queue_manager_config_file", help="path to the queue_manager config file",
        type=str,default="~/.queue_manager.cfg")
    parser.add_argument("action", help="action to take", type=str, choices=["report_completion"])
    parser.add_argument("-completed_queue_type_id", "-cqti",
        help="when reporting a completion, ID of the queue_type that was completed",
        type=str, default=None)
    parser.add_argument("-plate_id", "-pid",
        help="carry out the specified action for this plate", type=str, default=None)
    return parser


def report_completion_using_config(plate_id, completed_queue_type_id, queue_manager_config_path):

    logger.debug("queue_manager_config_path:  {}".format(queue_manager_config_path))

    conn = open_database_connection(queue_manager_config_path)
    cursor = conn.cursor()

    report_completion(cursor, plate_id, completed_queue_type_id)

    conn.commit()
    cursor.close()
    conn.close()


def report_completion(cursor, plate_id, completed_queue_type_id):
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


def open_database_connection(queue_manager_config_path):
    logger.debug("queue_manager_config_path:  {}".format(queue_manager_config_path))

    cp = ConfigParser.RawConfigParser()
    cp.read(queue_manager_config_path)

    sqlite3_file_path = cp.get("Database", "sqlite3_file_path")
    logger.debug("sqlite3_file_path:  {}".format(sqlite3_file_path))

    conn = sqlite3.connect(sqlite3_file_path)

    return conn


def main(args):
    if args.action == "report_completion":
        report_completion(args.plate_id, args.completed_queue_type_id, args.queue_manager_config_file)


if __name__ == "__main__":
    args = build_parser().parse_args(sys.argv[1:])
    args.queue_manager_config_file = os.path.expanduser(args.queue_manager_config_file)

    setup_logger.setup(verbose=args.verbose)

    logger.debug("args:  {}".format(args))

    main(args)
