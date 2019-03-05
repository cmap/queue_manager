import argparse
import ConfigParser
import logging
import os
import sys
import subprocess

import caldaia.utils.mysql_utils as mu
import caldaia.utils.config_tools as config_tools
import caldaia.utils.orm.lims_plate_orm as lpo

import broadinstitute.queue_manager.setup_logger as setup_logger
import sqs_queues.exceptions as qmExceptions
import sqs_queues.sqs_utils as sqs_utils
from sqs_queues.CommanderTemplate import CommanderTemplate


logger = logging.getLogger(setup_logger.LOGGER_NAME)


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    config_tools.add_config_file_options_to_parser(parser)
    config_tools.add_options_to_override_config(parser, ['hostname', 'data_path', 'scan_done_elapsed_time',
                                                         'queue_manager_config_filepath'])

    parser.add_argument('-machine_barcode')
    parser.add_argument('-espresso_path', help='path to espresso repo', default='/cmap/tools/jenkins/job_repos/espresso')

    return parser

def main(args):
    db = mu.DB(config_filepath=args.config_filepath, config_section=args.config_section).db
    cursor = db.cursor()

    this = RoastCommander(cursor, args.data_path, args.espresso_path, args.machine_barcode)
    this.execute_command()

def make_job(args):
    db = mu.DB(config_filepath=args.config_filepath, config_section=args.config_section).db
    cursor = db.cursor()

    return RoastCommander(cursor, args.data_path, args.espresso_path, args.machine_barcode)


class RoastCommander(CommanderTemplate):
    def __init__(self, cursor, base_path, espresso_path, machine_barcode):
        super(RoastCommander, self).__init__(base_path, espresso_path)
        self.lims_plate_orm = lpo.get_by_machine_barcode(cursor, machine_barcode)
        self.plate = self.lims_plate_orm.det_plate

        self._build_paths()
        self.command = self._build_command()

    def _build_paths(self):
        self.project_directory = os.path.join(self.base_path, self.lims_plate_orm.project_code)
        self.map_dir_path = os.path.join(self.base_path, 'map_src')
        self.lxb_dir_path = os.path.join(self.project_directory, 'lxb', self.plate)
        self.roast_dir_path = os.path.join(self.project_directory, 'roast')

    def _build_command(self):
        cd_cmd = '"cd ' + os.path.join(self.espresso_path, 'roast')
        roast_cmd = """roast('clean', true,
                            'plate', '{}',
                            'plate_path', '{}',
                            'map_path', '{}',
                            'raw_path', '{}',
                            'parallel', true)""".format(self.plate, self.roast_dir_path,
                                                        self.map_dir_path, self.lxb_dir_path)
        full_cmd = 'nohup matlab -nodesktop -nosplash -r ' + cd_cmd + '; ' + roast_cmd + '; quit" < /dev/null'
        return full_cmd



if __name__ == '__main__':
    args = build_parser().parse_args(sys.argv[1:])
    config_tools.add_config_file_settings_to_args(args)

    setup_logger.setup(verbose=args.verbose, log_file=args.log_path)

    main(args)