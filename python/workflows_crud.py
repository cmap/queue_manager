import logging
import setup_logger
import argparse
import workflow_template_orm
import workflow_orm
import argparse
import sys
import ConfigParser
import sqlite3


logger = logging.getLogger(setup_logger.LOGGER_NAME)


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-verbose', '-v', help='Whether to print a bunch of output.',
        action='store_true', default=False)
    parser.add_argument("-queue_manager_config_file", help="path to the queue_manager config file",
        type=str,default="queue_manager.cfg")
    parser.add_argument("action", help="action to take", type=str, choices=["create",
        "list"])
    parser.add_argument("-workflow_template_name", "-wtn",
        help="name of workflow template to use when creating workflows",
        type=str, default=None)
    parser.add_argument("-plate_ids", "-pids",
        help="create workflows for the specified plates using the workflow template based on the -workflow_template_name option",
        type=str, nargs="+", default=None)
    parser.add_argument("-dont_commit", help="don't commit any database changes",
        type=str, default=False)
    return parser


def create(conn, dont_commit, workflow_template_name, plate_ids):
    cursor = conn.cursor()
    wto = workflow_template_orm.get_by_name(cursor, workflow_template_name)

    for pid in plate_ids:
        for pair in wto.queue_type_pairs:
            prev_qt_id = pair[0].id
            next_qt_id = pair[1].id
            wo = workflow_orm.WorkflowOrm(plate_id=pid, prev_queue_type_id=prev_qt_id,
                next_queue_type_id=next_qt_id)
            wo.create(cursor)

    if dont_commit:
        conn.rollback()
    else:
        conn.commit()


def list():
    pass


def main(args):
    config = read_config(args.queue_manager_config_file)

    conn = sqlite3.connect(config.get("Database", "sqlite3_file_path"))

    if args.action == "create":
        create(conn, args.dont_commit, args.workflow_template_name, args.plate_ids)
    elif args.action == "list":
        list()


def read_config(queue_manager_config_file):
    cp = ConfigParser.RawConfigParser()
    cp.read(queue_manager_config_file)
    return cp


if __name__ == "__main__":
	args = build_parser().parse_args(sys.argv[1:])

	setup_logger.setup(verbose=args.verbose)

	logger.debug("args:  {}".format(args))

	main(args)
