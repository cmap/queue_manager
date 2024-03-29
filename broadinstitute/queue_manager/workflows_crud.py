import logging
import setup_logger
import argparse
import workflow_template_orm
import workflow_orm
import sys
import ConfigParser
import sqlite3
import os.path
import sql_query_utils


logger = logging.getLogger(setup_logger.LOGGER_NAME)


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-verbose', '-v', help='Whether to print a bunch of output.',
        action='store_true', default=False)
    parser.add_argument("-queue_manager_config_file", help="path to the queue_manager config file",
        type=str,default="~/.queue_manager.cfg")
    parser.add_argument("action", help="action to take", type=str, choices=["create",
        "list_templates", "delete_by_pid"])
    parser.add_argument("-workflow_template_name", "-wtn",
        help="name of workflow template to use when creating workflows",
        type=str, default=None)
    parser.add_argument("-plate_ids", "-pids",
        help="carry out the specified action for these plates", type=str,
        nargs="+", default=None)
    parser.add_argument("-dont_commit", help="don't commit any database changes",
        type=str, default=False)
    return parser


def create(cursor, workflow_template_name, plate_ids):
    wto = workflow_template_orm.get_by_name(cursor, workflow_template_name)

    for pid in plate_ids:
        for pair in wto.queue_type_pairs:
            prev_qt_id = pair[0].id
            next_qt_id = pair[1].id
            wo = workflow_orm.WorkflowOrm(plate_id=pid, prev_queue_type_id=prev_qt_id,
                next_queue_type_id=next_qt_id)
            wo.create(cursor)


def list_templates(cursor):
    wtos = workflow_template_orm.get_all(cursor)
    for wto in wtos:
        print wto


def delete_by_plate_ids(cursor, plate_ids):
    in_clause = sql_query_utils.build_in_clause(plate_ids)
    logger.debug("in_clause:  {}".format(in_clause))
    delete_stmt = "delete from workflow where plate_id in ({})".format(in_clause)
    logger.debug("delete_stmt:  {}".format(delete_stmt))
    cursor.execute(delete_stmt)


def main(args):
    config = read_config(args.queue_manager_config_file)

    conn = sqlite3.connect(config.get("Database", "sqlite3_file_path"))
    cursor = conn.cursor()

    if args.action == "create":
        create(cursor, args.workflow_template_name, args.plate_ids)
    elif args.action == "list_templates":
        list_templates(cursor)
    elif args.action == "delete_by_pids":
        delete_by_plate_ids(cursor, args.plate_ids)

    if args.dont_commit:
        conn.rollback()
    else:
        conn.commit()

    cursor.close()
    conn.close()


def read_config(queue_manager_config_file):
    cp = ConfigParser.RawConfigParser()
    cp.read(queue_manager_config_file)
    return cp


def validate_args(args):
    if args.action == "create":
        if args.workflow_template_name is None or args.plate_ids is None:
            logger.warning("for action create must provie workflow_template_name and plate_ids.  workflow_tempalte_name:  {}  plate_ids:  {}".format(args.workflow_template_name, args.plate_ids))
            return False

    return True


if __name__ == "__main__":
    args = build_parser().parse_args(sys.argv[1:])
    args.queue_manager_config_file = os.path.expanduser(args.queue_manager_config_file)

    setup_logger.setup(verbose=args.verbose)

    logger.debug("args:  {}".format(args))

    if validate_args(args):
        main(args)

