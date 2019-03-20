import argparse
import ConfigParser
import logging
import os
import sys
import importlib

import caldaia.utils.mysql_utils as mu
import caldaia.utils.config_tools as config_tools
import caldaia.utils.orm.lims_plate_orm as lpo


import broadinstitute.queue_manager.setup_logger as setup_logger
import sqs_queues.sqs_utils as sqs_utils
import sqs_queues.exceptions as qmExceptions
import sqs_queues.jobs_orm as jobs
import sqs_queues.yeezy as yeezy

logger = logging.getLogger(setup_logger.LOGGER_NAME)

unlinked_plate_insert_statement = "INSERT INTO unlinked_plate (unknown_barcode, status) VALUES (%s, %s)"

def build_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    config_tools.add_config_file_options_to_parser(parser)
    config_tools.add_options_to_override_config(parser, ['lxb2jcsv_path','hostname', 'archive_path', 'queue_manager_config_filepath', 'data_path'])

    parser.add_argument('--queue', '-q', help="queue to kick off", choices=['yeezy','kim','validator','roast', 'brew'])
    parser.add_argument('--jenkins_id', help='build number passed in from jenkins', type=int, required=True, default=None)
    return parser

def main(args):
    db = mu.DB(config_filepath=args.config_filepath, config_section=args.config_section).db
    cursor = db.cursor()

    j = QueueManager(queue=args.queue, jenkins_id=args.jenkins_id, queue_manager_config_filepath=args.queue_manager_config_filepath)

    if j.work_to_do:
        j.start_job(cursor)

class QueueManager(object):
    def __init__(self, queue, jenkins_id, queue_manager_config_filepath='~/queue_manager.cfg'):
        self.work_to_do = True
        self.queue = queue
        self.jenkins_id = jenkins_id
        self.job_entry = None
        self.queue_manager_config_filepath = queue_manager_config_filepath
        self.queue_config = self.get_queue_config()
        # self.get_message()
        # if self.message is not None:
        #     self.work_to_do = True

    def get_message(self):
        self.message = sqs_utils.receive_message_from_sqs_queue(self.queue_config['queue_url'])

    def get_queue_config(self):
        cp = ConfigParser.ConfigParser()
        if os.path.exists(self.queue_manager_config_filepath):
            cp.read(self.queue_manager_config_filepath)
            self.queue_workflow_info = dict(cp.items("workflow info"))
            queue_config = dict(cp.items(self.queue))
            return queue_config

        else:
            raise qmExceptions.NoConfigFileExistsAtGivenLocation(
                "Invalid Config Location: {}".format(self.queue_manager_config_filepath))

    def update_job_table(self, cursor):
        self.plate = lpo.get_by_machine_barcode(cursor, self.message.machine_barcode)
        if self.plate is not None:
            self.job_entry = jobs.update_or_create_job_entry(cursor, machine_barcode=self.message.machine_barcode,
                                            queue=self.queue, jenkins_id=self.jenkins_id)
        else:
            self.handle_unlinked_plate(cursor)

    def handle_unlinked_plate(self, cursor):
        # YEEZY IS ONLY THE CHECKER, KIM WILL REQUIRE PLATE ENTRY - MANUAL CURATION
        if self.queue == "yeezy":
            pass
        elif self.queue == "kim":
            cursor.execute(unlinked_plate_insert_statement, (self.message.machine_barcode, "unresolved"))
            sqs_utils.consume_message_from_sqs_queue(self.message)
        else:
            raise qmExceptions.UnassociatedPlateMadeItPastKim("Plate {} found in {} queue".format(self.message.machine_barcode, self.queue))

    def start_job(self, cursor):
        # self.update_job_table(cursor)
        queue_job = importlib.import_module(self.queue_config["job"].tolower())
        job_args_parser = getattr(queue_job, "build_parser")
        job_args = job_args_parser().parse_args(['-machine_barcode', self.message.machine_barcode])
        config_tools.add_config_file_settings_to_args(job_args)
        logger.info("job args: {}".format(job_args))

        make_job = getattr(queue_job, "make_job")
        jobObj = make_job(job_args)
        try:
            jobObj.execute_command()
        except:
            self.flag_job()

    def flag_job(self):
        if self.job_entry is not None:
            self.job_entry.toggle_flag()


    def finish_job(self):
        next_queue_index = self.queue_workflow_info['workflow'].index(self.queue) + 1
        self.queue = self.queue_workflow_info['workflow'][next_queue_index]
        next_queue_config = self.get_queue_config()
        self.message.pass_to_next_queue(next_queue_config)



if __name__ == "__main__":
    args = build_parser().parse_args(sys.argv[1:])
    config_tools.add_config_file_settings_to_args(args)

    setup_logger.setup(verbose=True)

    main(args)