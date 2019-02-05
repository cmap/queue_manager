import unittest
import logging
import mock

import pestle.io.setup_logger as setup_logger

import caldaia.utils.orm.lims_plate_orm as lpo
import sqs_queues.sqs_utils as utils

import sqs_queues.yeezy as yeezy

logger = logging.getLogger(setup_logger.LOGGER_NAME)

OG_SQS_boto = yeezy.sqs_utils.boto3
OG_SQS_receive = yeezy.sqs_utils.receive_messages_from_sqs_queue
OG_SQS_message_pass = yeezy.sqs_utils.Message.pass_to_next_queue
OG_LPO_get = yeezy.qscan.lpo.get_by_machine_barcode
OG_scan_num_lxbs = yeezy.qscan.Scan.get_num_lxbs_scanned
OG_scan_csv = yeezy.qscan.Scan.get_csv_path
OG_scan_last_lxb = yeezy.qscan.Scan.check_last_lxb_addition

class TestYeezy(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        yeezy.sqs_utils.boto3 = mock.Mock()
        yeezy.sqs_utils.receive_messages_from_sqs_queue = mock.Mock(return_value=TestYeezy.build_messages())
        yeezy.sqs_utils.Message.pass_to_next_queue = mock.Mock()
        yeezy.qscan.lpo.get_by_machine_barcode = mock.Mock()
        yeezy.qscan.Scan.get_num_lxbs_scanned = mock.Mock()
        yeezy.qscan.Scan.get_csv_path = mock.Mock()
        yeezy.qscan.Scan.check_last_lxb_addition = mock.Mock()

    @classmethod
    def tearDownClass(cls):
        yeezy.sqs_utils.boto3 = OG_SQS_boto
        yeezy.sqs_utils.receive_messages_from_sqs_queue = OG_SQS_receive
        yeezy.sqs_utils.Message.pass_to_next_queue = OG_SQS_message_pass
        yeezy.qscan.lpo.get_by_machine_barcode = OG_LPO_get
        yeezy.qscan.Scan.get_num_lxbs_scanned = OG_scan_num_lxbs
        yeezy.qscan.Scan.get_csv_path = OG_scan_csv
        yeezy.qscan.Scan.check_last_lxb_addition = OG_scan_last_lxb

    @staticmethod
    def build_args():
        args = yeezy.build_parser().parse_args(['--queue_manager_config_filepath', './queue_manager.cfg',
                                         '--hostname', 'babies'])
        logger.debug(args)
        return args

    @staticmethod
    def build_lims_plate_orm(machine_barcode):
        num = machine_barcode.rsplit("_",1)[1]
        l = lpo.LimsPlateOrm()
        l.machine_barcode = machine_barcode
        l.rna_plate = 'rna_plate' + "_" + num

        return l

    @staticmethod
    def build_messages():
        dict1 = {"Body": 'machine_barcode_1',
                "ReceiptHandle": 'receipt_handle_1'}

        dict2 = {"Body": 'machine_barcode_2',
                "ReceiptHandle": 'receipt_handle_2'}

        list_of_messages = [utils.Message(dict1, 'yeezy_url_1'),
                            utils.Message(dict2, 'yeezy_url_2')]
        return list_of_messages

    @staticmethod
    def common_setup_check_scan_done(num_lxbs_scanned, last_lxb_addition, csv_path):
        args = TestYeezy.build_args()
        message = TestYeezy.build_messages()[1]
        kim_queue = {"queue_url": "kim_url", "tag": "kim_tag"}
        yeezy.qscan.lpo.get_by_machine_barcode.return_value = TestYeezy.build_lims_plate_orm(message.machine_barcode)

        yeezy.qscan.Scan.get_num_lxbs_scanned.return_value = num_lxbs_scanned
        yeezy.qscan.Scan.check_last_lxb_addition.return_value = last_lxb_addition
        yeezy.qscan.Scan.get_csv_path.return_value = csv_path

        return (args, message, kim_queue)

    def setUp(self):
        pass

    def tearDown(self):
        yeezy.qscan.lpo.get_by_machine_barcode.reset_mock()
        yeezy.qscan.Scan.get_num_lxbs_scanned.reset_mock()
        yeezy.qscan.Scan.check_last_lxb_addition.reset_mock()
        yeezy.qscan.Scan.get_csv_path.reset_mock()
        yeezy.sqs_utils.Message.pass_to_next_queue.reset_mock()

    def test_all_happy_test_check_scan_done(self):
        logger.info("babies")
        # Setup mock return values
        (args, message, kim_queue) = TestYeezy.common_setup_check_scan_done(384, 1, True)

        logger.info(yeezy.qscan.Scan.get_num_lxbs_scanned.return_value)
        scan_is_done = yeezy.check_scan_done(args, None, message, kim_queue)
        self.assertEqual(scan_is_done, True)

        yeezy.qscan.lpo.get_by_machine_barcode.assert_called_once()
        yeezy.qscan.Scan.get_num_lxbs_scanned.assert_called_once()
        yeezy.qscan.Scan.get_csv_path.assert_called_once()
        yeezy.qscan.Scan.check_last_lxb_addition.assert_called_once()

        yeezy.sqs_utils.Message.pass_to_next_queue.assert_called_once()
        message_pass = yeezy.sqs_utils.Message.pass_to_next_queue.call_args_list
        self.assertEqual(message_pass, [mock.call(kim_queue)])


    def test_unhappy_num_lxbs_check_scan_done(self):
        # Setup mock return values
        (args, message, kim_queue) = TestYeezy.common_setup_check_scan_done(200, 1, True)


        scan_is_done = yeezy.check_scan_done(args, None, message, kim_queue)
        self.assertEqual(scan_is_done, False)

        yeezy.qscan.lpo.get_by_machine_barcode.assert_called_once()
        yeezy.qscan.Scan.get_num_lxbs_scanned.assert_called_once()
        yeezy.qscan.Scan.get_csv_path.assert_called_once()
        yeezy.qscan.Scan.check_last_lxb_addition.assert_called_once()

        self.assertFalse(yeezy.sqs_utils.Message.pass_to_next_queue.called)

    def test_unhappy_csv_path_check_scan_done(self):
        (args, message, kim_queue) = TestYeezy.common_setup_check_scan_done(384, 1, None)

        scan_is_done = yeezy.check_scan_done(args, None, message, kim_queue)
        self.assertEqual(scan_is_done, False)

        yeezy.qscan.lpo.get_by_machine_barcode.assert_called_once()
        yeezy.qscan.Scan.get_num_lxbs_scanned.assert_called_once()
        yeezy.qscan.Scan.get_csv_path.assert_called_once()
        yeezy.qscan.Scan.check_last_lxb_addition.assert_called_once()

        self.assertFalse(yeezy.sqs_utils.Message.pass_to_next_queue.called)

    def test_unhappy_csv_and_unhappy_num_lxb_check_scan_done(self):
        (args, message, kim_queue) = TestYeezy.common_setup_check_scan_done(200, 1, None)

        scan_is_done = yeezy.check_scan_done(args, None, message, kim_queue)
        self.assertEqual(scan_is_done, False)

        yeezy.qscan.lpo.get_by_machine_barcode.assert_called_once()
        yeezy.qscan.Scan.get_num_lxbs_scanned.assert_called_once()
        yeezy.qscan.Scan.get_csv_path.assert_called_once()
        yeezy.qscan.Scan.check_last_lxb_addition.assert_called_once()

        self.assertFalse(yeezy.sqs_utils.Message.pass_to_next_queue.called)

    def test_unhappy_lxb_happy_elapsed_time(self):
        (args, message, kim_queue) = TestYeezy.common_setup_check_scan_done(200, 86401, True)

        scan_is_done = yeezy.check_scan_done(args, None, message, kim_queue)
        self.assertEqual(scan_is_done, True)

        yeezy.qscan.lpo.get_by_machine_barcode.assert_called_once()
        yeezy.qscan.Scan.get_num_lxbs_scanned.assert_called_once()
        yeezy.qscan.Scan.get_csv_path.assert_called_once()
        yeezy.qscan.Scan.check_last_lxb_addition.assert_called_once()

        yeezy.sqs_utils.Message.pass_to_next_queue.assert_called_once()
        message_pass = yeezy.sqs_utils.Message.pass_to_next_queue.call_args_list
        self.assertEqual(message_pass, [mock.call(kim_queue)])



if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    unittest.main()