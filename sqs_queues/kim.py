"""
Copies LXBs from archive location to local project directory (creating directory structure if it does not exist).
Makes JCSV in plate LXB directory, disregarding CSV in archive location.
Renames LXBs to human-readable name and updates plate table in LIMS.

"""

import sys
import os
import argparse
import logging
import glob
import shutil


import caldaia.utils.mysql_utils as mu
import caldaia.utils.config_tools as config_tools

import pestle.data_ninja.lims.rename_plate_files as rpf

import broadinstitute.queue_manager.setup_logger as setup_logger
import sqs_queues.ScanInfo as si
import sqs_queues.exceptions as qmExceptions

logger = logging.getLogger(setup_logger.LOGGER_NAME)


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-machine_barcode', required=True)
    config_tools.add_config_file_options_to_parser(parser)
    config_tools.add_options_to_override_config(parser, ['lxb2jcsv_path','hostname', 'archive_path','data_path'])
    return parser

def main(args):
    db = mu.DB(config_filepath=args.config_filepath, config_section=args.config_section).db
    cursor = db.cursor()

    this = Kim(cursor, args.archive_path, args.data_path, args.lxb2jcsv_path, args.machine_barcode)
    this.execute_command()

def make_job(args):
    db = mu.DB(config_filepath=args.config_filepath, config_section=args.config_section).db
    cursor = db.cursor()

    return Kim(cursor, args.archive_path, args.data_path, args.lxb2jcsv_path, args.machine_barcode)

class Kim(si.ScanInfo):
    def __init__(self, cursor, archive_path, data_path, lxb2jcsv_path, machine_barcode):
        super(Kim, self).__init__(cursor, archive_path, machine_barcode)
        self.base_data_path = data_path
        self.lxb2jcsv_path = lxb2jcsv_path
        self.is_dev = self.check_for_dev()
        (self.destination_project_dir, self.destination_lxb_dir) = self.set_destination_dirs()

    def check_for_dev(self):
        if self.lims_plate_orm is not None:
            return False
        if self.plate_search_name.startswith("DEV"):
            return True
        else:
            msg = "The following plate : {} is not viable for processing".format(self.plate_search_name)
            raise qmExceptions.PlateCannotBeProcessed(msg)

    def set_destination_dirs(self):
        if self.is_dev:
            destination_project_dir = os.path.join(self.base_data_path, 'DEV')
            destination_lxb_dir = os.path.join(destination_project_dir, 'lxb', self.plate_search_name)
        else:
            destination_project_dir = os.path.join(self.base_data_path, self.lims_plate_orm.project_code)
            destination_lxb_dir = os.path.join(destination_project_dir, 'lxb', self.lims_plate_orm.det_plate)

        return (destination_project_dir, destination_lxb_dir)

    def setup_project_directory_structure_if_needed(self):
        # CHECK IF PROJECT DIRECTORY EXISTS AND SET UP DIRECTORY STRUCTURE IF NOT
        if not os.path.exists(self.destination_project_dir):
            os.mkdir(self.destination_project_dir)
            for subdir in ['lxb', 'map_src', 'maps', 'roast', 'brew', 'cup']:
                os.mkdir(os.path.join(self.destination_project_dir, subdir))
            return True
        return False
    def _num_lxbs_at_destination(self):
        return len(glob.glob(os.path.join(self.destination_lxb_dir, self.lims_plate_orm.det_plate, '*.lxb'))) > 0

    def copy_lxbs_to_project_directory(self):
        # MOVE ALL LXBs FROM ARCHIVE LOCATION
        src_lxb_file_list = glob.glob(os.path.join(self.lxb_path, '*.lxb'))
        src_lxb_file_list.sort()
        num_src_lxb_files = len(src_lxb_file_list)

        for (i, src_lxb_file) in enumerate(src_lxb_file_list):
            dest_lxb_file = os.path.join(self.destination_lxb_dir, os.path.basename(src_lxb_file))

            shutil.copyfile(src_lxb_file, dest_lxb_file)

            if i > 0 and i % 10 == 0:
                logger.debug("copying progress - working on {} out of {}".format(i, num_src_lxb_files))

        return True

    def _jcsv_at_destination(self):
        return os.path.exists(os.path.join(self.destination_lxb_dir, self.lims_plate_orm.det_plate, "*.jcsv"))

    def make_jcsv_in_lxb_directory(self):
        filename = self.plate_search_name + ".jcsv"
        outfile = os.path.join(self.destination_lxb_dir, filename)

        cmd = '{0} -i {1} -o {2}'.format(self.lxb2jcsv_path, self.destination_lxb_dir, outfile)
        try:
            logger.info("cmd:  {}".format(cmd))
            retval = os.system(cmd)
            logger.info('lxb2jcsv returned {}'.format(retval))

        except Exception as e:
            logger.exception("failed to make_csv.  stacktrace:  ")
            raise qmExceptions.FailureOccuredDuringProcessing(e)

        return outfile

    def make_lims_database_updates(self):
        # SET FIELD IN ORM OBJECT
        rna_plate_fields = [self.lims_plate_orm.pert_plate, self.lims_plate_orm.cell_id,
                            self.lims_plate_orm.pert_time,
                            self.lims_plate_orm.rep_num]
        self.lims_plate_orm.rna_plate = "_".join(rna_plate_fields)
        self.lims_plate_orm.det_plate = self.lims_plate_orm.rna_plate + "_" + self.lims_plate_orm.bead_set
        self.lims_plate_orm.scan_det_plate = self.lims_plate_orm.original_barcode + "_" + self.lims_plate_orm.bead_set

        # USE ORM OBJECT TO UPDATE DATABASE
        self.lims_plate_orm.update_in_db(self.cursor)
        return self.lims_plate_orm

    def execute_command(self):
        # SET UP PATHS AND DIRECTORY STRUCTURE IF D.N.E.
        self.created_project_dir = self.setup_project_directory_structure_if_needed()
        self.moved_lxbs = self.copy_lxbs_to_project_directory() if self._num_lxbs_at_destination() < self.num_lxbs_scanned else False
        self.made_jcsv = self.make_jcsv_in_lxb_directory() if self._jcsv_at_destination() is False else False
        if self.moved_lxbs and self.made_jcsv:
            self.make_lims_database_updates()
            rpf.rename_files(self.lims_plate_orm.det_plate, self.destination_lxb_dir)
            return True
        return False
        #todo or should raise exception ?


if __name__ == '__main__':
    args = build_parser().parse_args(sys.argv[1:])
    config_tools.add_config_file_settings_to_args(args)

    setup_logger.setup(verbose=args.verbose, log_file=args.log_path)

    main(args)