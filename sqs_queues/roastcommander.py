import argparse
import logging
import os
import shutil
import sys

import caldaia.utils.mysql_utils as mu
import caldaia.utils.config_tools as config_tools
import caldaia.utils.orm.lims_plate_orm as lpo

import broadinstitute.queue_manager.setup_logger as setup_logger
import sqs_queues.exceptions as qmExceptions

from sqs_queues.CommanderTemplate import CommanderTemplate


logger = logging.getLogger(setup_logger.LOGGER_NAME)


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    config_tools.add_config_file_options_to_parser(parser)
    config_tools.add_options_to_override_config(parser, ['hostname', 'data_path', 'scan_done_elapsed_time',
                                                         'queue_manager_config_filepath'])

    parser.add_argument('-machine_barcode', help='for production use')
    parser.add_argument('-espresso_path', help='path to espresso repo', default='/cmap/tools/jenkins/job_repos/espresso')

    parser.add_argument('-det_plate', help='name of plate to roast', type=str)
    parser.add_argument('-deprecate', help='flag to deprecate rather than delete', action='store_true')
    return parser

def main(args):
    db = mu.DB(config_filepath=args.config_filepath, config_section=args.config_section).db
    cursor = db.cursor()

    plate = lpo.get_by_det_plate(cursor, args.det_plate)
    if plate:
        this = RoastCommander(args.data_path, args.espresso_path, plate.det_plate, plate.project_code, args.deprecate)
    else:
        plate = lpo.LimsPlateOrm()
        plate.parse_det_plate(args.det_plate)
        this = RoastCommander(args.data_path, args.espresso_path, plate.det_plate, plate.project_code, args.deprecate)

    this.execute_command()

def make_job(args):
    db = mu.DB(config_filepath=args.config_filepath, config_section=args.config_section).db
    cursor = db.cursor()

    plate = lpo.get_by_machine_barcode(cursor, args.machine_barcode)

    return RoastCommander(args.data_path, args.espresso_path, plate.det_plate, plate.project_code, args.deprecate)


class RoastCommander(CommanderTemplate):
    def __init__(self, base_path, espresso_path, det_plate, project_id, deprecate):
        super(RoastCommander, self).__init__(base_path, espresso_path)
        self.plate = det_plate
        self.project_id = project_id

        self.do_deprecate = deprecate
        self._build_paths()
        self._build_command()

    def _build_paths(self):
        self.project_directory = os.path.join(self.base_path, self.project_id)
        self.map_src_dir_path = os.path.join(self.project_directory, 'map_src')
        self.maps_path = os.path.join(self.project_directory, 'maps')
        self.lxb_dir_path = os.path.join(self.project_directory, 'lxb')
        self.roast_dir_path = os.path.join(self.project_directory, 'roast')
        self.plate_roast_dir_path = os.path.join(self.roast_dir_path, self.plate)

    def _check_for_preexisting_roast(self):
        if os.path.exists(self.plate_roast_dir_path):
            if self.do_deprecate:
                logger.info("Roast already exists -- deprecating")
                if not os.path.exists(os.path.join(self.roast_dir_path, "deprecated")):
                        os.mkdir(os.path.join(self.roast_dir_path, "deprecated"))
                shutil.move(self.plate_roast_dir_path, os.path.join(self.roast_dir_path, "deprecated", self.replicate_set_name))
            else:
                logger.info("Roast already exists -- deleting")
                shutil.rmtree(self.plate_roast_dir_path)

    def _build_command(self):
        cd_cmd = '"cd ' + os.path.join(self.espresso_path, 'roast')
        roast_cmd = """roast('clean', true, ...
                            'plate', '{}', ...
                            'plate_path', '{}', ...
                            'map_path', '{}', ...
                            'raw_path', '{}', ...
                            'parallel', true)""".format(self.plate, self.roast_dir_path,
                                                        self.maps_path, self.lxb_dir_path)
        self.command = 'matlab -nodesktop -nosplash -r ' + cd_cmd + '; ' + roast_cmd + '; quit" < /dev/null'

        logger.info("Command built : {}".format(self.command))


if __name__ == '__main__':
    args = build_parser().parse_args(sys.argv[1:])
    config_tools.add_config_file_settings_to_args(args)

    setup_logger.setup(verbose=args.verbose, log_file=args.log_path)

    main(args)