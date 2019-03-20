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
    config_tools.add_options_to_override_config(parser, ['lxb2jcsv_path','hostname', 'archive_path','data_path'])
    config_tools.add_config_file_options_to_parser(parser)
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
        self.check_for_dev()
        self.set_destination_dirs()

    def check_for_dev(self):
        if self.lims_plate_orm is not None:
            self.is_dev = False
        elif self.plate_search_name.startswith("DEV"):
            logger.info("{} identified as a DEV plate".format(self.plate_search_name))
            self.is_dev = True
        else:
            msg = "The following plate : {} is not viable for processing".format(self.plate_search_name)
            raise qmExceptions.PlateCannotBeProcessed(msg)

    def set_destination_dirs(self):
        self.destination_project_dir = os.path.join(self.base_data_path, 'DEV') if self.is_dev else os.path.join(self.base_data_path, self.lims_plate_orm.project_code)
        if self.is_dev:
            self.destination_lxb_dir = os.path.join(self.destination_project_dir, 'lxb', self.plate_search_name)
        else:
            self.build_plate_values()
            self.destination_lxb_dir = os.path.join(self.destination_project_dir, 'lxb', self.plate_search_name)
    def check_lxb_destination(self):
        if self._jcsv_at_destination() or self._num_lxbs_at_destination() > 0:
            logger.info("Found existing directory for plate -- deprecating")
            if not os.path.exists(os.path.join(self.destination_project_dir, "lxb", "deprecated")):
                os.mkdir(os.path.join(self.destination_project_dir,"lxb", "deprecated"))
            shutil.move(self.destination_lxb_dir,
                        os.path.join(self.destination_project_dir, "lxb", "deprecated", self.plate_search_name))

        os.mkdir(self.destination_lxb_dir)

    def setup_project_directory_structure_if_needed(self):
        # CHECK IF PROJECT DIRECTORY EXISTS AND SET UP DIRECTORY STRUCTURE IF NOT
        if not os.path.exists(self.destination_project_dir):
            os.mkdir(self.destination_project_dir)
            for subdir in ['lxb', 'map_src', 'maps', 'roast', 'brew', 'cup']:
                os.mkdir(os.path.join(self.destination_project_dir, subdir))
            return True
        return False

    def _num_lxbs_at_destination(self):
        return len(glob.glob(os.path.join(self.destination_lxb_dir, '*.lxb')))

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
        return os.path.exists(os.path.join(self.destination_lxb_dir, "*.jcsv"))

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

    def build_plate_values(self):
        # SET FIELD IN ORM OBJECT
        rna_plate_fields = [self.lims_plate_orm.pert_plate, self.lims_plate_orm.cell_id,
                            self.lims_plate_orm.pert_time,
                            self.lims_plate_orm.replicate]
        self.lims_plate_orm.rna_plate = "_".join(rna_plate_fields)
        self.lims_plate_orm.det_plate = self.lims_plate_orm.rna_plate + "_" + self.lims_plate_orm.bead_set
        self.lims_plate_orm.scan_det_plate = self.lims_plate_orm.original_barcode + "_" + self.lims_plate_orm.bead_set
        return self.lims_plate_orm

    def make_lims_database_updates(self):

        # USE ORM OBJECT TO UPDATE DATABASE
        self.lims_plate_orm.update_in_db(self.cursor)

    def execute_command(self):
        # SET UP PATHS AND DIRECTORY STRUCTURE IF D.N.E.
        self.setup_project_directory_structure_if_needed()
        self.check_lxb_destination()
        self.copy_lxbs_to_project_directory()
        self.make_jcsv_in_lxb_directory()
        if not self.is_dev:
            self.make_lims_database_updates()
            rpf.rename_files(self.lims_plate_orm.det_plate, os.path.join(self.destination_project_dir, 'lxb'))


if __name__ == '__main__':
    args = build_parser().parse_args(sys.argv[1:])
    config_tools.add_config_file_settings_to_args(args)

    setup_logger.setup(verbose=True)

    main(args)