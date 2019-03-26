import argparse
import datetime
import logging
import os
import shutil
import tempfile
import sys

import caldaia.utils.mysql_utils as mu
import caldaia.utils.config_tools as config_tools
import caldaia.utils.orm.lims_plate_orm as lpo
import caldaia.utils.orm.replicate_set_plate_orm as rspo

import broadinstitute.queue_manager.setup_logger as setup_logger
import sqs_queues.exceptions as qmExceptions

from sqs_queues.CommanderTemplate import CommanderTemplate


logger = logging.getLogger(setup_logger.LOGGER_NAME)


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    config_tools.add_config_file_options_to_parser(parser)
    config_tools.add_options_to_override_config(parser, ['hostname', 'data_path', 'scan_done_elapsed_time',
                                                         'queue_manager_config_filepath'])

    parser.add_argument('-replicate_set_id', type=int)
    plate_input = parser.add_mutually_exclusive_group()
    plate_input.add_argument('-replicate_set_name', help='name of the replicate set of the desired brew, used to obtain replicates from the database', type=str)
    plate_input.add_argument('-plate_grp_path', '-plates', help='path to grp with list of plates to include', type=str)

    parser.add_argument('-zmad_ref', help='Which z-scores to brew by', type=str, choices=['vc', 'pc'], default='pc')
    parser.add_argument('-group_by', help='How to collapse in brew', type=str, default='pert_id,pert_dose')

    parser.add_argument('-pod_dir', help='override standard production directory', type=str)
    parser.add_argument('-deprecate', help='flag to deprecate rather than delete', action='store_true')

    parser.add_argument('-espresso_path', help='path to espresso repo', default='/cmap/tools/jenkins/job_repos/espresso')

    return parser

def main(args):
    db = mu.DB(config_filepath=args.config_filepath, config_section=args.config_section).db
    cursor = db.cursor()

    if args.replicate_set_name:
        replicate_set_list = rspo.get_replicate_set_plate_orms_in_rep_set_by_name(cursor, args.replicate_set_name)
    else:
        replicate_set_list = get_replicates_from_grp(cursor, args.plate_grp_path)

    base_path = args.pod_dir if args.pod_dir else args.data_path


    this = BrewCommander(cursor, base_path, args.espresso_path, replicate_set_list, args.zmad_ref, args.group_by, args.deprecate)
    this.execute_command()


def get_replicates_from_grp(cursor, path_to_grp):
    replicates = []
    with open(path_to_grp, "r") as grp:
        for line in grp:
            orm = lpo.get_by_det_plate(cursor, line.strip())
            if orm is None:
                orm = lpo.LimsPlateOrm()
                orm.parse_det_plate()
            replicates.append(orm)
    return replicates

def make_job(args):
    db = mu.DB(config_filepath=args.config_filepath, config_section=args.config_section).db
    cursor = db.cursor()

    replicate_set_list = rspo.get_replicate_set_plate_orms_in_rep_set_by_replicate_set_id(cursor, args.replicate_set_id)
    return BrewCommander(cursor, args.data_path, args.espresso_path, replicate_set_list, args.zmad_ref, args.group_by, args.deprecate)


class BrewCommander(CommanderTemplate):
    def __init__(self, cursor, base_path, espresso_path, replicate_set_list, zmad_ref, group_by, deprecate):

        super(BrewCommander, self).__init__(cursor, base_path, espresso_path)

        self.replicate_set_list = replicate_set_list

        self.project_id = self.replicate_set_list[0].project_code
        self.replicate_set_name = self.replicate_set_list[0].brew_prefix
        self.zmad_ref = zmad_ref
        self.group_by = group_by
        self.do_deprecate = deprecate


        self._build_paths()
        self.command = self._build_command()


    def _build_paths(self):
        self.plate_path = os.path.join(self.base_path,self.project_id,"roast")
        self.brew_path = os.path.join(self.base_path, self.project_id, "brew", self.zmad_ref)
        self.replicate_brew_dir_path = os.path.join(self.brew_path, self.replicate_set_name)

        self.brew_cmd_dir = tempfile.mkdtemp(prefix="{}_".format(self.replicate_set_name))
        logger.debug("temp location for storing brew_cmd file and grp files - brew_cmd_dir:  {}".format(self.brew_cmd_dir))

        self.plate_grp_file = os.path.join(self.brew_cmd_dir, "{}_plates.grp".format(self.replicate_set_name))
        logger.debug("plate_grp_file:  {}".format(self.plate_grp_file))

        self.brew_cmd_file = os.path.join(self.brew_cmd_dir,"{}_brew_cmd.m".format(self.replicate_set_name))

    def _check_for_preexisting_brew(self):
        if os.path.exists(self.replicate_brew_dir_path):
            if self.do_deprecate:
                if not os.path.exists(os.path.join(self.brew_path, "deprecated")):
                        os.mkdir(os.path.join(self.brew_path, "deprecated"))
                shutil.move(self.replicate_brew_dir_path, os.path.join(self.brew_path, "deprecated", self.replicate_set_name))
            else:
                shutil.rmtree(self.replicate_brew_dir_path)

    def _write_plate_grp(self):
        with open(self.plate_grp_file, "w") as include_grp:
            # NB: UNIQUE CONSTRAINT ON MACHINE_BARCODE-REP_SET_ID PAIR
            for replicate in self.replicate_set_list:
                if replicate.is_included_in_brew == 1:
                    include_grp.write("\n".join(replicate.lims_plate_orm.det_plate) + "\n")

    def _build_command(self):
        cd_cmd = "cd {}".format(os.path.join(self.espresso_path, "brew"))

        brew_cmd = """brew('plate', '{plate}', ...
                   'plate_path', '{plate_path}', ...
                   'brew_path', '{brew_path}', ...
                   'group_by', '{group_by}', ...
                   'zmad_ref', '{zmad_ref}', ...
                   'filter_vehicle', 'false', ...
                   'clean', true, ...
                   'include','{include_grp}')""".format(plate=self.plate_grp_file, plate_path=self.plate_path, brew_path=self.brew_path,
                                                        group_by=self.group_by, zmad_ref=("ZS" + self.zmad_ref.upper()), include_grp=self.plate_grp_file)
        full_cmd = """
               {cd_cmd}

               {brew_cmd}
               """.format(cd_cmd=cd_cmd, brew_cmd=brew_cmd)

        with open(self.brew_cmd_file, "w") as f:
            f.write(full_cmd + "\n")

        self.command = "matlab -nodesktop -nosplash -nojit -nodisplay < {brew_cmd_file}".format(brew_cmd_file=self.brew_cmd_file)

        logger.info("Command built : {}".format(full_cmd))

    def _post_build_failure(self):
        for replicate in self.replicate_set_list:
            det_plate = replicate.lims_plate_orm.det_plate
            self.cursor.execute("update plate set brew_error=%s where det_plate=%s", (self.error, det_plate, ))

    def _post_build_success(self):
        date = datetime.datetime.today()
        for replicate in self.replicate_set_list:
            det_plate = replicate.lims_plate_orm.det_plate
            self.cursor.execute("update plate set is_brewed=1, brew_date=%s where det_plate=%s", (date, det_plate))



if __name__ == '__main__':
    # set up arguments
    args = build_parser().parse_args(sys.argv[1:])
    setup_logger.setup(verbose=True)

    logger.debug("args:  {}".format(args))

    main(args)