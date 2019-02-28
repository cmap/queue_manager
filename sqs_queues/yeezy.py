"""
Polls Yeezy SQS queue for newly scanned plates added by lumitracker.
Checks 'status' of scan based on number of LXBs and amount of time passed since last LXB addition
(disregards CSVs from scanner).
If scan is done, sends plate Kim SQS queue.

"""
import sys
import argparse
import logging

import caldaia.utils.mysql_utils as mu
import caldaia.utils.config_tools as config_tools

import broadinstitute.queue_manager.setup_logger as setup_logger

from sqs_queues.ScanInfo import ScanInfo
import sqs_queues.exceptions as qmExceptions

logger = logging.getLogger(setup_logger.LOGGER_NAME)


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p = parser.add_mutually_exclusive_group()
    p.add_argument('-machine_barcode', required=True)
    config_tools.add_config_file_options_to_parser(parser)
    config_tools.add_options_to_override_config(parser, ['hostname','archive_path','scan_done_elapsed_time'])

    return parser

def main(args):
    db = mu.DB(config_filepath=args.config_filepath, config_section=args.config_section).db
    cursor = db.cursor()

    this = Yeezy(cursor, args.archive_path, int(args.scan_done_elapsed_time), args.machine_barcode)
    if this.scan_done:
        pass
    else:
        raise qmExceptions.YeezyReportsScanNotDone("Plate {} not ready for Kim".format(plate_info.plate_search_name))

class Yeezy(ScanInfo):
    def __init__(self, cursor, archive_path, scan_done_elapsed_time, machine_barcode):
        ScanInfo.__init__(cursor, archive_path, machine_barcode)

        self.scan_done_elapsed_time = scan_done_elapsed_time
        self.elapsed_time = None

        self.scan_done = None
        (self.scan_done, self.elapsed_time) = self.check_scan_done()

    def check_scan_done(self):
        '''
        figure out if scan is finished. either there are 384 lxbs or the last
        update to any file happened more than 2 hrs ago
        '''
        if self.num_lxbs_scanned == 0:
            return (False, None)

        # didn't have 0, check if scanning has stopped
        elapsed_time = self.check_last_lxb_addition()
        if elapsed_time is None:
            return (False, None)

        is_scan_done = False
        if self.num_lxbs_scanned >= 384 :
            is_scan_done = True
        elif self.num_lxbs_scanned < 384 :
            is_scan_done = elapsed_time > self.scan_done_elapsed_time


        logger.info(
            "self.num_lxbs_scanned:  {}   elapsed_time: {}  self.scan_done_elapsed_time:  {}  is_scan_done:  {}".format(
                self.num_lxbs_scanned, elapsed_time, self.scan_done_elapsed_time, is_scan_done))

        return (is_scan_done, elapsed_time)

    def execute_command(self):
        if self.scan_done:
            return True
        return False

if __name__ == '__main__':
    args = build_parser().parse_args(sys.argv[1:])
    config_tools.add_config_file_settings_to_args(args)

    setup_logger.setup(verbose=True)

    main(args)
