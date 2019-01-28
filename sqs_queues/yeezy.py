"""
polls sqs Yeezy
queries rsync
Checks 'status' of scan, i.e. num LXBs scanned, etc.
If scan done, sends plate Kim SQS

"""
import sys
import os
import argparse
import logging
import threading
import ConfigParser

import caldaia.utils.mysql_utils as mu
import caldaia.utils.config_tools as config_tools
import caldaia.utils.orm.lims_plate_orm as lpo

import pestle.io.setup_logger as setup_logger

import sqs_queues.scan as scan
import sqs_queues.sqs_utils as sqs_utils

logger = logging.getLogger(setup_logger.LOGGER_NAME)


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    config_tools.add_config_file_settings_to_args(parser)
    config_tools.add_options_to_override_config(parser, ['hostname', 'scan_done_elapsed_time', 'archive_path', 'queue_manager_config_filepath'])
    return parser

def main(args):

    db = mu.DB(host=args.hostname).db
    cursor = db.cursor()

    cp = ConfigParser.ConfigParser()
    if os.path.exists(args.queue_manager_config_filepath):
        cp.read(args.queue_manager_config_filepath)

    yeezy_queue_url = cp.get('yeezy', 'queue_url')
    kim_queue = cp.items('kim')

    messages = sqs_utils.receive_messages_from_sqs_queue(yeezy_queue_url)

    for message in messages:
        lims_plate_orm = lpo.get_by_machine_barcode(cursor, message.machine_barcode)
        thread = threading.Thread(target=check_scan_done, args=(args, message, kim_queue, lims_plate_orm))


def check_scan_done(args, message, kim_queue, lims_plate_orm):
    plate_info = scan.Scan(args.archive_path, args.scan_done_elapsed_time, lims_plate_orm=lims_plate_orm)
    if plate_info.scan_done:
        sqs_utils.consume_message_from_sqs_queue(message)
        sqs_utils.send_message_to_sqs_queue(kim_queue['queue_url'], lims_plate_orm.machine_barcode, kim_queue['tag'])

if __name__ == '__main__':
    args = build_parser().parse_args(sys.argv[1:])
    config_tools.add_config_file_settings_to_args(args)

    setup_logger.setup(verbose=args.verbose, log_file=args.log_path)

    main(args)
