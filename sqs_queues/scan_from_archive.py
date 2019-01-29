'''
class to describe an active scan
'''

import os
import glob
import time
import logging
import pestle.io.setup_logger as setup_logger

import caldaia.utils.orm.lims_plate_orm as lpo

logger = logging.getLogger(setup_logger.LOGGER_NAME)

class ScanFromArchive(object):
    def __init__(self, cursor, archive_path, scan_done_elapsed_time, machine_barcode):

        self.archive_path = archive_path
        self.scan_done_elapsed_time = scan_done_elapsed_time
        self.lxb_path = None
        self.csv_path = None
        self.num_lxbs_scanned = None
        self.scan_done = None
        self.elapsed_time = None

        self.lims_plate_orm = lpo.get_by_machine_barcode(cursor, machine_barcode)

        if self.lims_plate_orm:
            self.plate_search_name = self.lims_plate_orm.rna_plate
        else:
            self.plate_search_name = machine_barcode

        self.lxb_path = os.path.join(self.archive_path, "lxb", self.plate_search_name + "*")
        self.csv_path = self.get_csv_path()
        self.num_lxbs_scanned = self.get_num_lxbs_scanned()
        (self.scan_done, self.elapsed_time) = self.check_scan_done()


    def __str__(self):
        return " ".join(["{}:{}".format(k, v) for (k, v) in self.__dict__.items()])

    def get_num_lxbs_scanned(self):
        '''
        return the number of lxbs scanned for the plate
        '''
        return len(glob.glob(os.path.join(self.lxb_path, '*.lxb')))

    def get_csv_path(self):
        '''
        find and return the path to the csv file for the given plate
        '''

        csv_search = os.path.join(self.archive_path, "csv", self.plate_search_name + "*.csv")
        logger.debug("csv_search:  {}".format(csv_search))

        csv_paths = glob.glob(csv_search)
        logger.debug("csv_paths:  {}".format(csv_paths))

        if len(csv_paths) == 0:
            return None
        elif len(csv_paths) == 1:
            return csv_paths[0]
        else:
            logger.info("multiple csv's found, choosing newest")
            logger.debug([(x, os.path.getmtime(x)) for x in csv_paths])
            return max(csv_paths, key=lambda x: os.path.getmtime(x))

    def check_last_lxb_addition(self):
        lxb_files = glob.glob(os.path.join(self.lxb_path, '*'))
        now = time.time()
        try:
            max_mtime = max([os.path.getmtime(x) for x in lxb_files])

            elapsed_time = now - max_mtime
            logger.info(
                "elapsed time since last lxb file modification (in seconds): {}".format(elapsed_time))
            return elapsed_time
        except Exception as e:
            logger.exception("failed to getmtime for lxb files.  stacktrace:  ")
            return None

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
        if self.num_lxbs_scanned >= 384 and self.csv_path is not None:
            logger.info('here')
            is_scan_done = True
        elif self.num_lxbs_scanned < 384 and self.csv_path is not None:
            is_scan_done = elapsed_time > self.scan_done_elapsed_time


        elif (self.num_lxbs_scanned >= 384 and self.csv_path is None) or \
                (self.num_lxbs_scanned < 384  and elapsed_time > self.scan_done_elapsed_time):
            self.make_csv()

        logger.info(
            "self.num_lxbs_scanned:  {}  self.csv_path:  {}  elapsed_time: {}  self.scan_done_elapsed_time:  {}  is_scan_done:  {}".format(
                self.num_lxbs_scanned, self.csv_path, elapsed_time, self.scan_done_elapsed_time, is_scan_done))

        return (is_scan_done, elapsed_time)

    def make_csv(self):
        # todo ?
        pass