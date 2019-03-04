import unittest
import logging
import mock

import pestle.io.setup_logger as setup_logger

import caldaia.utils.orm.lims_plate_orm as lpo
import caldaia.utils.config_tools as config_tools

import sqs_queues.sqs_utils as utils

import sqs_queues.yeezy as yeezy

logger = logging.getLogger(setup_logger.LOGGER_NAME)


OG_LPO_get = yeezy.si.lpo.get_by_machine_barcode
OG_scan_num_lxbs = yeezy.Yeezy.get_num_lxbs_scanned
OG_scan_last_lxb = yeezy.Yeezy.check_last_lxb_addition

test_barcode = 'test_barcode'

class TestYeezy(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        yeezy.si.lpo.get_by_machine_barcode = mock.Mock()
        yeezy.Yeezy.get_num_lxbs_scanned = mock.Mock()
        yeezy.Yeezy.check_last_lxb_addition = mock.Mock()


    @classmethod
    def tearDownClass(cls):
        yeezy.si.lpo.get_by_machine_barcode = OG_LPO_get
        yeezy.Yeezy.get_num_lxbs_scanned = OG_scan_num_lxbs
        yeezy.Yeezy.check_last_lxb_addition = OG_scan_last_lxb


    def tearDown(self):
        yeezy.si.lpo.get_by_machine_barcode.reset_mock()
        yeezy.Yeezy.get_num_lxbs_scanned.reset_mock()
        yeezy.Yeezy.check_last_lxb_addition.reset_mock()


    @staticmethod
    def build_args():
        args = yeezy.build_parser().parse_args(['-machine_barcode', test_barcode])
        config_tools.add_config_file_settings_to_args(args)
        return args


    @staticmethod
    def build_lims_plate_orm(machine_barcode):
        num = machine_barcode.rsplit("_",1)[1]
        l = lpo.LimsPlateOrm()
        l.det_plate = 'det_plate'
        l.machine_barcode = machine_barcode
        l.rna_plate = 'rna_plate' + "_" + num

        return l


    @staticmethod
    def common_setup_Yeezy(num_lxbs_scanned, last_lxb_addition):
        args = TestYeezy.build_args()
        cursor = mock.Mock()
        yeezy.si.lpo.get_by_machine_barcode.return_value = TestYeezy.build_lims_plate_orm(args.machine_barcode)
        yeezy.Yeezy.get_num_lxbs_scanned.return_value = num_lxbs_scanned
        yeezy.Yeezy.check_last_lxb_addition.return_value = last_lxb_addition
        this = yeezy.Yeezy(cursor, args.archive_path, args.scan_done_elapsed_time, args.machine_barcode)

        return (this, args)


    def test_Yeezy__init__(self):
        (test_yeezy, args) = TestYeezy.common_setup_Yeezy(200, 666)
        self.assertEqual(test_yeezy.plate_search_name, 'det_plate')
        self.assertEqual(test_yeezy.scan_done_elapsed_time, int(args.scan_done_elapsed_time))
        self.assertEqual(test_yeezy.elapsed_time, None)
        self.assertEqual(test_yeezy.scan_done, None)


    def test_unhappy_lpo_check_scan_done(self):
        (testYeezy, args) = TestYeezy.common_setup_Yeezy(384, 1)
        cursor = mock.Mock()
        cursor.execute = mock.Mock()

        yeezy.si.lpo.get_by_machine_barcode.return_value = None
        (scan_is_done, elapsed_time) = testYeezy.check_scan_done()
        self.assertEqual(scan_is_done, True)

        yeezy.si.lpo.get_by_machine_barcode.assert_called_once()
        yeezy.Yeezy.get_num_lxbs_scanned.assert_called_once()
        yeezy.Yeezy.check_last_lxb_addition.assert_called_once()


    def test_all_happy_test_check_scan_done(self):
        # Setup mock return values
        (testYeezy, args) = TestYeezy.common_setup_Yeezy(384, 1)
        cursor = mock.Mock()
        cursor.execute = mock.Mock()

        (scan_is_done, elapsed_time) = testYeezy.check_scan_done()
        self.assertEqual(scan_is_done, True)

        yeezy.si.lpo.get_by_machine_barcode.assert_called_once()
        yeezy.Yeezy.get_num_lxbs_scanned.assert_called_once()
        yeezy.Yeezy.check_last_lxb_addition.assert_called_once()


    def test_unhappy_num_lxbs_check_scan_done(self):
        # Setup mock return values
        (testYeezy, args) = TestYeezy.common_setup_Yeezy(200, 1)

        (scan_is_done, elapsed_time) = testYeezy.check_scan_done()
        self.assertEqual(scan_is_done, False)

        yeezy.si.lpo.get_by_machine_barcode.assert_called_once()
        yeezy.Yeezy.get_num_lxbs_scanned.assert_called_once()
        yeezy.Yeezy.check_last_lxb_addition.assert_called_once()


    def test_unhappy_lxb_happy_elapsed_time(self):
        (testYeezy, args) = TestYeezy.common_setup_Yeezy(200, 86401)

        (scan_is_done, elapsed_time) = testYeezy.check_scan_done()
        self.assertEqual(scan_is_done, True)

        yeezy.si.lpo.get_by_machine_barcode.assert_called_once()
        yeezy.Yeezy.get_num_lxbs_scanned.assert_called_once()
        yeezy.Yeezy.check_last_lxb_addition.assert_called_once()

    def test_happy_execute_command(self):
        (testYeezy, args) = TestYeezy.common_setup_Yeezy(200, 86401)
        response = testYeezy.execute_command()
        self.assertTrue(response)

    def test_unhappy_execute_command(self):
        (testYeezy, args) = TestYeezy.common_setup_Yeezy(200, 1)
        with self.assertRaises(yeezy.qmExceptions.YeezyReportsScanNotDone):
            testYeezy.execute_command()

if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    unittest.main()