import unittest
import mock
import logging

import pestle.io.setup_logger as setup_logger


import sqs_queues.yeezy as yeezy
import sqs_queues.sqs_utils as utils
import caldaia.utils.orm.lims_plate_orm as lpo

logger = logging.getLogger(setup_logger.LOGGER_NAME)


OG_SQS_receive = yeezy.sqs_utils.receive_messages_from_sqs_queue
OG_SQS_message_pass = yeezy.sqs_utils.Message.pass_to_next_queue
OG_LPO_get = yeezy.scan.lpo.get_by_machine_barcode
OG_scan_num_lxbs = yeezy.scan.Scan.get_num_lxbs_scanned
OG_scan_csv = yeezy.scan.Scan.get_csv_path
OG_scan_last_lxb = yeezy.scan.Scan.check_last_lxb_addition

class TestYeezy(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        yeezy.sqs_utils.receive_messages_from_sqs_queue = mock.Mock(return_value=TestYeezy.build_messages())
        yeezy.sqs_utils.Message.pass_to_next_queue = mock.Mock()
        yeezy.scan.lpo.get_by_machine_barcode = mock.Mock()
        yeezy.scan.Scan.get_num_lxbs_scanned = mock.Mock()
        yeezy.scan.Scan.get_csv_path = mock.Mock()
        yeezy.scan.Scan.check_last_lxb_addition = mock.Mock()

    @classmethod
    def tearDownClass(cls):
        yeezy.sqs_utils.receive_messages_from_sqs_queue = OG_SQS_receive
        yeezy.sqs_utils.Message.pass_to_next_queue = OG_SQS_message_pass
        yeezy.scan.lpo.get_by_machine_barcode = OG_LPO_get
        yeezy.scan.Scan.get_num_lxbs_scanned = OG_scan_num_lxbs
        yeezy.scan.Scan.get_csv_path = OG_scan_csv
        yeezy.scan.Scan.check_last_lxb_addition = OG_scan_last_lxb

    @staticmethod
    def build_args():
        args = yeezy.build_parser().parse_args(['--queue_manager_config_filepath', './queue_manager.cfg',
                                         '--hostname', 'babies'])
        return args

    @staticmethod
    def build_lims_plate_orm(machine_barcode):
        num = machine_barcode.rsplit("_",1)[1]
        l = lpo.LimsPlateOrm()
        l.machine_barcode = machine_barcode
        l.rna_plate = 'rna_plate' + num
        l.is_yanked = 'false'

        return l

    @staticmethod
    def build_messages():
        m = utils.Message()
        m.machine_barcode = 'machine_barcode_1'
        m.receipt_handle = 'receipt_handle_1'
        m.current_queue_url = 'yeezy_url_1'

        m2 = utils.Message()
        m2.machine_barcode = 'machine_barcode_2'
        m2.receipt_handle = 'receipt_handle_2'
        m2.current_queue_url = 'yeezy_url_2'

        list_of_messages = [m, m2]
        return list_of_messages

    @staticmethod
    def common_setup_check_scan_done(num_lxbs_scanned, last_lxb_addition, csv_path):
        args = TestYeezy.build_args()
        message = TestYeezy.build_messages()[1]
        kim_queue = {"queue_url": "kim_url", "tag": "kim_tag"}
        yeezy.scan.lpo.get_by_machine_barcode.return_value = TestYeezy.build_lims_plate_orm(message.machine_barcode)

        yeezy.scan.Scan.get_num_lxbs_scanned.return_value = num_lxbs_scanned
        yeezy.scan.Scan.check_last_lxb_addition.return_value = last_lxb_addition
        yeezy.scan.Scan.get_csv_path.return_value = csv_path

        return (args, message, kim_queue)

    def setUp(self):
        logger.info("babies")

    def tearDown(self):
        yeezy.scan.Scan.get_num_lxbs_scanned.reset_mock()
        yeezy.scan.Scan.check_last_lxb_addition.reset_mock()
        yeezy.scan.Scan.get_csv_path.reset_mock()
        yeezy.sqs_utils.Message.pass_to_next_queue.reset_mock()

    def happy_test_check_scan_done(self):

        # Setup mock return values
        (args, message, kim_queue) = TestYeezy.common_setup_check_scan_done(384, 1, True)

        scan_is_done = yeezy.check_scan_done(args, None, message, kim_queue)
        self.assertEqual(scan_is_done, True)

        yeezy.scan.lpo.get_by_machine_barcode.assert_called_once()
        yeezy.scan.Scan.get_num_lxbs_scanned.assert_called_once()
        yeezy.scan.Scan.get_csv_path.assert_called_once()
        yeezy.scan.Scan.check_last_lxb_addition.assert_called_once()

        yeezy.sqs_utils.Message.pass_to_next_queue.assert_called_once()
        message_pass = vars(yeezy.sqs_utils.Message.pass_to_next_queue.call_args[0][0])
        self.assertEqual(message, mock.call(kim_queue))



    def unhappy_num_lxbs_check_scan_done(self):
        # Setup mock return values
        (args, message, kim_queue) = TestYeezy.common_setup_check_scan_done(200, 1, True)


        scan_is_done = yeezy.check_scan_done(args, None, message, kim_queue)
        self.assertEqual(scan_is_done, False)


if __name__ == "__main__":
    setup_logger.setup(verbose=True)
    unittest.main()