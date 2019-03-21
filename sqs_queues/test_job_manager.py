import unittest
import mock
import os
import logging
import types


import sqs_queues.job_manager as jm
from sqs_queues.yeezy import Yeezy
from  sqs_queues.kim import Kim
import sqs_queues.roastcommander as roastcommander
import sqs_queues.brewcommander as brewcommander

import broadinstitute.queue_manager.setup_logger as setup_logger
logger = logging.getLogger(setup_logger.LOGGER_NAME)

test_queue = 'kim'
test_jenkins_id = -666
test_barcode='806090'

OG_Config_read = jm.ConfigParser.ConfigParser.read
OG_Config_items = jm.ConfigParser.ConfigParser.items

def find_queue_manager_config():
    current_location = os.getcwd()
    for root, dirs, files in os.walk(current_location):
        if "queue_manager.cfg" in files:
            return os.path.join(root, "queue_manager.cfg")

DEFAULT_CONFIG_PATH = find_queue_manager_config()



class TestJobManager(unittest.TestCase):


    @classmethod
    def setUpClass(cls):
        pass

    def setUp(self):
       pass
    @classmethod
    def tearDownClass(cls):
       pass

    @staticmethod
    def common_job_manager_setup(queue=test_queue, jenkins_id=test_jenkins_id):
        j = jm.JobManager(queue, jenkins_id, queue_manager_config_filepath=DEFAULT_CONFIG_PATH)
        return j

    def test__init__(self):
        job = TestJobManager.common_job_manager_setup()
        self.assertEqual(job.queue, test_queue)
        self.assertEqual(job.jenkins_id, test_jenkins_id)
        self.assertEqual(job.queue_manager_config_filepath, DEFAULT_CONFIG_PATH)
        self.assertFalse(job.work_to_do)
        self.assertIsNone(job.job_entry)
        self.assertIsNotNone(job.queue_config)

    def test_get_message(self):
        # SET UP MOCK FOR HAPPY CONDITION
        OG_sqs_message = jm.sqs_utils.receive_message_from_sqs_queue
        jm.sqs_utils.receive_message_from_sqs_queue = mock.Mock(return_value=True)

        job = TestJobManager.common_job_manager_setup()
        job.get_message()
        self.assertTrue(job.work_to_do)

        # SET UP MOCK FOR UNHAPPY CONDITION
        jm.sqs_utils.receive_message_from_sqs_queue.return_value=None
        job = TestJobManager.common_job_manager_setup()
        job.get_message()
        self.assertFalse(job.work_to_do)

        # TEAR DOWN MOCKS
        jm.sqs_utils.receive_message_from_sqs_queue = OG_sqs_message

    def test__get_queue_config(self):
        # HAPPY CONFIG PATH
        job = TestJobManager.common_job_manager_setup()
        # jm.ConfigParser.ConfigParser.items.reset_mock()

        job._get_queue_config()

        self.assertIsNotNone(job.queue_workflow_info)
        self.assertIsNotNone(job.queue_config)

        # UNHAPPY CONFIG PATH
        job.queue_manager_config_filepath = "babies"
        with self.assertRaises(jm.qmExceptions.NoConfigFileExistsAtGivenLocation):
            job._get_queue_config()

    def test_update_job_table(self):
        # SETUP MOCKS
        OG_lpo = jm.lpo.get_by_machine_barcode
        OG_update_job = jm.jobs.update_or_create_job_entry

        jm.lpo.get_by_machine_barcode = mock.Mock(return_value=True)
        jm.jobs.update_or_create_job_entry = mock.Mock()
        cursor = mock.Mock()
        # HAPPY PATH, LIMS PLATE ENTRY EXISTS
        job = TestJobManager.common_job_manager_setup()
        job.message = mock.Mock(machine_barcode='fake_machine_barcode')
        job.update_job_table(cursor)

        jm.jobs.update_or_create_job_entry.assert_called_with(cursor, machine_barcode="fake_machine_barcode",
                                                              jenkins_id=test_jenkins_id, queue=test_queue)
        self.assertIsNotNone(job.job_entry)

        # SET UP MOCKS FOR NEXT TEST
        jm.lpo.get_by_machine_barcode.return_value = None
        jm.jobs.update_or_create_job_entry.reset_mock()
        OG_handle = jm.JobManager.handle_unlinked_plate
        jm.JobManager.handle_unlinked_plate = mock.Mock()

        # UNHAPPY PATH, NO LIMS ENTRY FOR PLATE
        job = TestJobManager.common_job_manager_setup()
        job.message = mock.Mock(machine_barcode='fake_machine_barcode')
        job.update_job_table(cursor)

        jm.JobManager.handle_unlinked_plate.assert_called_with(cursor)
        jm.jobs.update_or_create_job_entry.assert_not_called

        jm.JobManager.handle_unlinked_plate = OG_handle
        jm.jobs.update_or_create_job_entry = OG_update_job
        jm.lpo.get_by_machine_barcode = OG_lpo

    def test_handle_unlinked_plate(self):
        # SET UP MOCKS
        cursor = mock.Mock()
        cursor.execute=mock.Mock()
        OG_consume_message = jm.sqs_utils.consume_message_from_sqs_queue
        jm.sqs_utils.consume_message_from_sqs_queue = mock.Mock()

        for queue in TestJobManager.common_job_manager_setup().queue_workflow_info:
            logger.debug("queue: {}".format(queue))
            job = TestJobManager.common_job_manager_setup(queue=queue)
            job.message = mock.Mock(machine_barcode='fake_machine_barcode')
            if queue == "yeezy":
                job.handle_unlinked_plate(cursor)
                cursor.execute.assert_not_called()
                jm.sqs_utils.consume_message_from_sqs_queue.assert_not_called()
            elif queue == "kim":
                job.handle_unlinked_plate(cursor)
                cursor.execute.assert_called_with(jm.unlinked_plate_insert_statement,
                                                  ("fake_machine_barcode", "unresolved"))
                jm.sqs_utils.consume_message_from_sqs_queue.assert_called_with(job.message)

            else :
                with self.assertRaises(jm.qmExceptions.UnassociatedPlateMadeItPastKim):
                    job.handle_unlinked_plate(cursor)
        # TEAR DOWN MOCKS
        jm.sqs_utils.consume_message_from_sqs_queue = OG_consume_message

    def test__make_job(self):
        for queue in TestJobManager.common_job_manager_setup().queue_workflow_info:
            logger.debug("queue: {}".format(queue))
            if queue == "validator" or queue == "brew":
                continue
            jobm = TestJobManager.common_job_manager_setup(queue=queue)
            jobm.message = mock.Mock(machine_barcode=test_barcode)
            jobm._make_job()
            self.assertIsNotNone(jobm.job)
            self.assertTrue(hasattr(jobm.job, "execute_command"), "queue: {} has no execute_command ".format(queue))
            self.assertEqual(jobm.job.__class__.__name__,  jobm.queue_config["job"])

    def test_start_job(self):
        cursor = mock.Mock()
        for queue in TestJobManager.common_job_manager_setup().queue_workflow_info:
            logger.debug("queue: {}".format(queue))
            if queue == "validator" or queue == "brew":
                continue
            jobm = TestJobManager.common_job_manager_setup(queue=queue)
            jobm.message = mock.Mock(machine_barcode=test_barcode)
            jobm.update_job_table = mock.Mock()
            # jobm._make_job()
            # jobm.job.execute_command = mock.Mock()
            jobm.start_job(cursor)
            # logger.debug("command executed {}".format(jobm.job.execute_command.call_args_list))

    def test_flag_job(self):

        OG_JE = jm.jobs.JobsOrm.toggle_flag
        jm.jobs.JobsOrm.toggle_flag = mock.Mock()

        test_job = TestJobManager.common_job_manager_setup()
        test_job.job_entry = mock.Mock(return_value=jm.jobs.JobsOrm.toggle_flag)

        # HAPPY PATH
        test_job.flag_job()
        jm.jobs.JobsOrm.toggle_flag.assert_called_once()

        jm.jobs.JobsOrm.toggle_flag.reset_mock()
        # UNHAPPY PATH NO JOB ENTRY
        test_job.job_entry.return_value = None
        test_job.flag_job()
        jm.jobs.JobsOrm.toggle_flag.assert_not_called()

        jm.jobs.JobsOrm.toggle_flag = OG_JE

    def test_finish_job(self):
        jm.sqs_utils.Message.pass_to_next_queue = mock.Mock()

        test_job = TestJobManager.common_job_manager_setup(queue="yeezy")
        test_job.message = mock.Mock(machine_barcode=test_barcode)
        test_job.finish_job()
        logger.debug( jm.sqs_utils.Message.pass_to_next_queue.call_args_list)
        jm.sqs_utils.Message.pass_to_next_queue.assert_called_once



if __name__ == '__main__':
    setup_logger.setup(verbose=True)

    unittest.main()
