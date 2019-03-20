'''
class to describe an active scan
'''

import os
import glob
import time
import logging

import broadinstitute.queue_manager.setup_logger as setup_logger
import caldaia.utils.orm.lims_plate_orm as lpo

logger = logging.getLogger(setup_logger.LOGGER_NAME)

class ScanInfo(object):
    def __init__(self, db, archive_path, machine_barcode):
        self.db = db
        self.cursor = self.db.cursor()
        self.archive_path = archive_path
        self.lxb_path = None
        self.num_lxbs_scanned = None

        self.lims_plate_orm = lpo.get_by_machine_barcode(self.db.cursor(), machine_barcode)

        if self.lims_plate_orm:
            self.plate_search_name = self.lims_plate_orm.det_plate
        else:
            self.plate_search_name = machine_barcode

        self._get_lxb_path()
        self.num_lxbs_scanned = self.get_num_lxbs_scanned()

    def __str__(self):
        return " ".join(["{}:{}".format(k, v) for (k, v) in self.__dict__.items()])

    def _get_lxb_path(self):
        self.lxb_path = os.path.join(self.archive_path, 'lxb', self.plate_search_name + '*')
        logger.info("Checking {} for LXBs".format(self.lxb_path))

    def get_num_lxbs_scanned(self):
        # GET THE NUMBER OF LXBS IN THE ARCHIVE DIRECTORY
        return len(glob.glob(os.path.join(self.lxb_path, '*.lxb')))

    def check_last_lxb_addition(self):
        # GET THE AMOUNT OF TIME ELAPSED BETWEEN THE LAST LXB ADDED TO ARCHIVE DIRECTORY AND NOW
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




