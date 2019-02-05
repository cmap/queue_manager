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
import pestle.io.setup_logger as setup_logger

import sqs_queues.queue_scan as qscan
import sqs_queues.sqs_utils as sqs_utils

logger = logging.getLogger(setup_logger.LOGGER_NAME)



def build_parser():

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    config_tools.add_config_file_options_to_parser(parser)
    config_tools.add_options_to_override_config(parser,
                                                ['hostname','archive_path','scan_done_elapsed_time', 'queue_manager_config_filepath'])

    return parser

def main(args):
    print args

    db = mu.DB(config_filepath=args.config_filepath, config_section=args.config_section).db
    cursor = db.cursor()

    cp = ConfigParser.ConfigParser()

    if os.path.exists(args.queue_manager_config_filepath):
        cp.read(args.queue_manager_config_filepath)

        yeezy_queue_url = cp.get('yeezy', 'queue_url')
        kim_queue = dict(cp.items('kim'))

        messages = sqs_utils.receive_messages_from_sqs_queue(yeezy_queue_url)

        if messages is not None:
            for message in messages:
                check_scan_done(args, cursor, message, kim_queue)
    else :
        #todo;
        Exception("babies")

def check_scan_done(args, cursor, message, kim_queue_config):
    # NB all args added with config_tools have type str
    plate_info = qscan.QueueScan(cursor, args.archive_path, int(args.scan_done_elapsed_time), machine_barcode=message.machine_barcode)
    print plate_info
    if plate_info.scan_done:
        message.pass_to_next_queue(kim_queue_config)
        return True

    return False

if __name__ == '__main__':
    args = build_parser().parse_args(sys.argv[1:])
    config_tools.add_config_file_settings_to_args(args)

    setup_logger.setup(verbose=True)

    main(args)
