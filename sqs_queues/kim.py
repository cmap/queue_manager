"""
Updates database (see database updates below) [possibly set lma_status = 'complete']
Copies LXBs to local project directory (creating directory structure if it does not exist)
makes JCSV in plate directory
Renames LXBs to human-readable name

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

import pestle.io.setup_logger as setup_logger
import pestle.data_ninja.lims.rename_plate_files as rpf


import sqs_queues.queue_scan as qscan
import sqs_queues.sqs_utils as sqs_utils

logger = logging.getLogger(setup_logger.LOGGER_NAME)


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    config_tools.add_config_file_settings_to_args(parser)
    config_tools.add_options_to_override_config(parser, ['lxb2jcsv_path','hostname', 'archive_path', 'queue_manager_config_filepath', 'data_path'])
    return parser

def main(args):
    db = mu.DB(config_filepath=args.config_filepath, config_section=args.config_section).db
    cursor = db.cursor()

    cp = ConfigParser.ConfigParser()
    if os.path.exists(args.queue_manager_config_filepath):

        cp.read(args.queue_manager_config_filepath)
        kim_queue = dict(cp.items("kim"))
        messages = sqs_utils.receive_messages_from_sqs_queue(kim_queue['queue_url'])

        if messages is not None:
            for message in messages:
                scan_info = qscan.QueueScan(cursor, args.archive_path, args.scan_done_elapsed_time, machine_barcode=message.machine_barcode)
                (destination_project_dir, destination_lxb_dir, is_dev) = setup_arguments(args.data_path, scan_info)
                copy_lxbs_to_project_directory(destination_lxb_dir, scan_info)

                if not is_dev:
                    make_jcsv_in_lxb_directory(destination_lxb_dir, scan_info)
                    rpf.rename_files(scan_info.lims_plate_orm.det_plate, destination_lxb_dir)
                    make_lims_database_updates(cursor, scan_info.lims_plate_orm)


def setup_arguments(data_path, scan_info):
    is_dev = False

    if scan_info.lims_plate_orm is None:
        if scan_info.plate_search_name.startswith("DEV"):
            is_dev = True
            destination_project_dir = os.path.join(data_path, "DEV")
            destination_lxb_dir = os.path.join(destination_project_dir, 'lxb', scan_info.plate_search_name)
        else:
            handle_plate_no_lims_database_entry(scan_info)
            sys.exit(666)
    else:
        # SET UP PATHS AND DIRECTORY STRUCTURE IF D.N.E.
        destination_project_dir = os.path.join(data_path, scan_info.lims_plate_orm.project_code)
        destination_lxb_dir = os.path.join(destination_project_dir, 'lxb', scan_info.lims_plate_orm.det_plate)
        setup_project_directory_structure_if_needed(destination_project_dir)

    return (destination_project_dir, destination_lxb_dir, is_dev)

def setup_project_directory_structure_if_needed(destination_project_dir):
    # CHECK IF PROJECT DIRECTORY EXISTS AND SET UP DIRECTORY STRUCTURE IF NOT
    if not os.path.exists(destination_project_dir):
        os.mkdir(destination_project_dir)
        for subdir in ['lxb', 'map_src', 'maps', 'roast', 'brew', 'cup']:
            os.mkdir(os.path.join(destination_project_dir, subdir))


def copy_lxbs_to_project_directory(destination_lxb_dir, scan_info):

    src_lxb_file_list = glob.glob(os.path.join(scan_info.lxb_path, '*.lxb'))
    src_lxb_file_list.sort()
    num_src_lxb_files = len(src_lxb_file_list)

    for (i, src_lxb_file) in enumerate(src_lxb_file_list):
        dest_lxb_file = os.path.join(destination_lxb_dir, os.path.basename(src_lxb_file))

        shutil.copyfile(src_lxb_file, dest_lxb_file)

        if i > 0 and i % 10 == 0:
            logger.debug("copying progress - working on {} out of {}".format(i, num_src_lxb_files))

def make_jcsv_in_lxb_directory(lxb2jcsv_path, destination_lxb_dir, scan_info):

    outfile = os.path.join(destination_lxb_dir, scan_info.lims_plate_orm.det_plate)

    cmd = '{0} -i {1} -o {2}'.format(lxb2jcsv_path, destination_lxb_dir, outfile)
    try:
        logger.info("cmd:  {}".format(cmd))
        retval = os.system(cmd)
        logger.info('lxb2jcsv returned {}'.format(retval))
    except Exception as e:
        logger.exception("failed to make_csv.  stacktrace:  ")

    return outfile

def check_lxb_corruption():
    pass

def handle_plate_no_lims_database_entry(scan_info):
    pass

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