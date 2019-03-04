import logging
import unittest
import mock
import time

import broadinstitute.queue_manager.setup_logger as setup_logger
import sqs_queues.ScanInfo as si

logging.getLogger(setup_logger.LOGGER_NAME)

glob_return = ['test1', 'test2', 'test3']

time_return = time.time()
now_return = 1560000000

# SAVING ORIGINAL CODE FOR FUNCTIONS TO BE MOCKED
og_glob = si.glob
og_lpo_get = si.lpo.get_by_machine_barcode
og_get_time = si.os.path.getmtime
og_time = time.time
# MOCKS USED IN TESTS
lpo = mock.Mock(det_plate='det_plate')

class TestScanInfo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        si.glob = mock.Mock()
        si.glob.glob = mock.Mock(return_value=glob_return)

        si.lpo.get_by_machine_barcode = mock.Mock()

        si.os.path.getmtime = mock.Mock(return_value=time_return)
        time.time = mock.Mock(return_value=now_return)

    def setUp(self):
        si.lpo.get_by_machine_barcode.return_value = lpo
    def tearDown(self):
        si.glob.glob.reset_mock()

    @classmethod
    def tearDownClass(cls):
        si.glob = og_glob
        si.lpo.get_by_machine_barcode = og_lpo_get

        si.lpo.getmtime = og_get_time
        si.time = og_time

    @staticmethod
    def build_scan(reset_glob=False):
        cursor = mock.Mock()
        scan = si.ScanInfo(cursor, 'archive_path', 'machine_barcode')
        if reset_glob:
            si.glob.glob.reset_mock()
        return scan

    def test___init__lpo_returned(self):

        scan = self.build_scan()
        self.assertEqual(scan.archive_path, 'archive_path')
        self.assertEqual(scan.lims_plate_orm, lpo)
        self.assertEqual(scan.plate_search_name, 'det_plate')
        self.assertEqual(scan.lxb_path, 'archive_path/lxb/det_plate*')
        si.glob.glob.assert_called_once()
        self.assertEqual(scan.num_lxbs_scanned, len(glob_return))
        glob_call = si.glob.glob.call_args_list[0]
        expected_call = mock.call('archive_path/lxb/det_plate*/*.lxb')
        self.assertEqual(glob_call, expected_call)

    def test__init__lpo_none(self):
        si.lpo.get_by_machine_barcode.return_value = None
        scan = self.build_scan()
        self.assertEqual(scan.archive_path, 'archive_path')
        self.assertEqual(scan.lims_plate_orm, None)
        self.assertEqual(scan.plate_search_name, 'machine_barcode')
        self.assertEqual(scan.lxb_path, 'archive_path/lxb/machine_barcode*')
        si.glob.glob.assert_called_once()
        self.assertEqual(scan.num_lxbs_scanned, len(glob_return))
        glob_call = si.glob.glob.call_args_list[0]
        expected_call = mock.call('archive_path/lxb/machine_barcode*/*.lxb')
        self.assertEqual(glob_call, expected_call)

    def test_get_num_lxbs_scanned(self):
        scan = self.build_scan(True)
        n_lxbs = scan.get_num_lxbs_scanned()
        self.assertEqual(n_lxbs, len(glob_return))
        si.glob.glob.assert_called_once()
        glob_call = si.glob.glob.call_args_list[0]
        expected_call = mock.call('archive_path/lxb/det_plate*/*.lxb')
        self.assertEqual(glob_call, expected_call)

    def test_check_last_lxb_addition_lpo(self):
        scan = self.build_scan(True)
        elapsed_time = scan.check_last_lxb_addition()
        self.assertEqual(elapsed_time, now_return-time_return)
        si.glob.glob.assert_called_once()
        glob_call = si.glob.glob.call_args_list[0]
        expected_call = mock.call('archive_path/lxb/det_plate*/*')
        self.assertEqual(glob_call, expected_call)

    def test_check_last_lxb_addition_lpo_none(self):
        si.lpo.get_by_machine_barcode.return_value = None
        scan = self.build_scan(True)
        elapsed_time = scan.check_last_lxb_addition()
        self.assertEqual(elapsed_time, now_return-time_return)
        si.glob.glob.assert_called_once()
        glob_call = si.glob.glob.call_args_list[0]
        expected_call = mock.call('archive_path/lxb/machine_barcode*/*')
        self.assertEqual(glob_call, expected_call)

if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    unittest.main()