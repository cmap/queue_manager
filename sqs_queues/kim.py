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
import ConfigParser

import caldaia.utils.mysql_utils as mu
import caldaia.utils.config_tools as config_tools

import pestle.data_ninja.lims.rename_plate_files as rpf

import broadinstitute.queue_manager.setup_logger as setup_logger
import sqs_queues.ScanInfo as qscan
import sqs_queues.sqs_utils as sqs_utils
import sqs_queues.jobs_orm as jobs
import sqs_queues.exceptions as qmExceptions

logger = logging.getLogger(setup_logger.LOGGER_NAME)


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-machine_barcode', required=True)
    config_tools.add_config_file_options_to_parser(parser)
    config_tools.add_options_to_override_config(parser, ['lxb2jcsv_path','hostname', 'archive_path', 'queue_manager_config_filepath', 'data_path'])
    return parser

def main(args):
    db = mu.DB(config_filepath=args.config_filepath, config_section=args.config_section).db
    cursor = db.cursor()

    scan_info = qscan.ScanInfo(cursor, args.archive_path, machine_barcode=args.machine_barcode)
    (destination_project_dir, destination_lxb_dir, is_dev) = sift_for_viable_jobs(args, scan_info)
    copy_lxbs_to_project_directory(destination_lxb_dir, scan_info)

    if is_dev:
        # DEV PLATES MOVED TO DEV PROJECT DIR WITHOUT RENAME; NO ENTRY IN LIMS TO UPDATE
        make_jcsv_in_lxb_directory(destination_lxb_dir, scan_info.plate_search_name)

    else:
        make_lims_database_updates(cursor, scan_info.lims_plate_orm)
        make_jcsv_in_lxb_directory(destination_lxb_dir, scan_info.lims_plate_orm.det_plate)
        rpf.rename_files(scan_info.lims_plate_orm.det_plate, destination_lxb_dir)

def sift_for_viable_jobs(args, scan_info):
    is_dev = False
    # LPO IS REQUIRED FOR KIM TO KNOW DESTINATION PATH
    if scan_info.lims_plate_orm is None:
        # EXCEPTION MADE FOR DEV PLATES, WHICH ARE NOT REGISTERED IN LIMS, BUT HAVE KNOWN PROJECT
        if scan_info.plate_search_name.startswith("DEV"):
            is_dev = True
            destination_project_dir = os.path.join(args.data_path, "DEV")
            destination_lxb_dir = os.path.join(destination_project_dir, 'lxb', scan_info.plate_search_name)
        else:
            msg = "The following plate : {} is not viable for processing".format(scan_info.plate_search_name)
            logger.info(msg)
            raise qmExceptions.PlateCannotBeProcessed(msg)
    else:
        destination_project_dir = os.path.join(args.data_path, scan_info.lims_plate_orm.project_code)
        destination_lxb_dir = os.path.join(destination_project_dir, 'lxb', scan_info.lims_plate_orm.det_plate)
        # SET UP PATHS AND DIRECTORY STRUCTURE IF D.N.E.
        setup_project_directory_structure_if_needed(destination_project_dir)

    return (destination_project_dir, destination_lxb_dir, is_dev)

def setup_project_directory_structure_if_needed(destination_project_dir):
    # CHECK IF PROJECT DIRECTORY EXISTS AND SET UP DIRECTORY STRUCTURE IF NOT
    if not os.path.exists(destination_project_dir):
        os.mkdir(destination_project_dir)
        for subdir in ['lxb', 'map_src', 'maps', 'roast', 'brew', 'cup']:
            os.mkdir(os.path.join(destination_project_dir, subdir))


def copy_lxbs_to_project_directory(destination_lxb_dir, scan_info):
    # MOVE ALL LXBs FROM ARCHIVE LOCATION
    src_lxb_file_list = glob.glob(os.path.join(scan_info.lxb_path, '*.lxb'))
    src_lxb_file_list.sort()
    num_src_lxb_files = len(src_lxb_file_list)

    for (i, src_lxb_file) in enumerate(src_lxb_file_list):
        dest_lxb_file = os.path.join(destination_lxb_dir, os.path.basename(src_lxb_file))

        shutil.copyfile(src_lxb_file, dest_lxb_file)

        if i > 0 and i % 10 == 0:
            logger.debug("copying progress - working on {} out of {}".format(i, num_src_lxb_files))

def make_jcsv_in_lxb_directory(lxb2jcsv_path, destination_lxb_dir, filename):

    outfile = os.path.join(destination_lxb_dir, filename)

    cmd = '{0} -i {1} -o {2}'.format(lxb2jcsv_path, destination_lxb_dir, outfile)
    try:
        logger.info("cmd:  {}".format(cmd))
        retval = os.system(cmd)
        logger.info('lxb2jcsv returned {}'.format(retval))

    except Exception as e:
        logger.exception("failed to make_csv.  stacktrace:  ")
        raise qmExceptions.FailureOccuredDuringProcessing(e)

    return outfile

def make_lims_database_updates(cursor, lims_plate_orm):
    # SET FIELD IN ORM OBJECT
    rna_plate_fields = [lims_plate_orm.pert_plate, lims_plate_orm.cell_id, lims_plate_orm.pert_time, lims_plate_orm.rep_num]
    lims_plate_orm.rna_plate =  "_".join(rna_plate_fields)
    lims_plate_orm.det_plate = lims_plate_orm.rna_plate + "_" + lims_plate_orm.bead_set
    lims_plate_orm.scan_det_plate = lims_plate_orm.original_barcode + "_" + lims_plate_orm.bead_set

    # USE ORM OBJECT TO UPDATE DATABASE
    lims_plate_orm.update_in_db(cursor)
    return lims_plate_orm

if __name__ == '__main__':
    args = build_parser().parse_args(sys.argv[1:])
    config_tools.add_config_file_settings_to_args(args)

    setup_logger.setup(verbose=args.verbose, log_file=args.log_path)

    main(args)