import unittest
import mock
import logging


import pestle.io.setup_logger as setup_logger


import sqs_queues.kim as kim
import sqs_queues.scan_from_archive as scan

import caldaia.utils.orm.lims_plate_orm as lpo

logger = logging.getLogger(setup_logger.LOGGER_NAME)


class TestKim(unittest.TestCase):


    @staticmethod
    def create_lims_plate_orm():
        l = lpo.LimsPlateOrm()

        l.pert_plate = 'pertPlate'
        l.cell_id = 'cellId'
        l.pert_time = 'pertTime'
        l.rep_num = 'repNum'
        l.bead_set = 'beadSet'
        l.rna_plate = 'rna_plate'
        l.det_plate = 'det_plate'
        l.scan_det_plate = 'scan_det_plate'
        l.original_barcode = 'original_barcode'

        return l

    @staticmethod
    def create_scan_from_archive():
        OG_LPO = scan.lpo.get_by_machine_barcode
        OG_scan_os = scan.os.path.join
        OG_scan_csv = scan.ScanFromArchive.get_csv_path
        OG_scan_get_num_lxb = scan.ScanFromArchive.get_num_lxbs_scanned
        OG_scan_done = scan.ScanFromArchive.check_scan_done

        scan.lpo.get_by_machine_barcode = mock.Mock(return_value=TestKim.create_lims_plate_orm())
        scan.os.path.join = mock.Mock(return_value='fake_lxb_path')
        scan.ScanFromArchive.get_csv_path = mock.Mock(return_value='fake_csv_path')
        scan.ScanFromArchive.get_num_lxbs_scanned = mock.Mock(return_value=384)
        scan.ScanFromArchive.check_scan_done = mock.Mock(return_value=(True, 1))

        fake_scan = scan.ScanFromArchive(None, "archive_path", "elapse_time", "machine_barcode")

        scan.lpo.get_by_machine_barcode = OG_LPO
        scan.os.path.join = OG_scan_os
        scan.ScanFromArchive.get_csv_path = OG_scan_csv
        scan.ScanFromArchive.get_num_lxbs_scanned = OG_scan_get_num_lxb
        scan.ScanFromArchive.check_scan_done = OG_scan_done

        return fake_scan

    def test_make_lims_database_updates(self):
        OG_LPO = kim.lpo.LimsPlateOrm.update_in_db

        cursor = mock.Mock()
        cursor.execute = mock.Mock()

        updated_entry = kim.make_lims_database_updates(cursor, TestKim.create_lims_plate_orm())

        cursor.execute.assert_called_once
        self.assertEqual(updated_entry.rna_plate, 'pertPlate_cellId_pertTime_repNum')
        self.assertEqual(updated_entry.det_plate, 'pertPlate_cellId_pertTime_repNum_beadSet')
        self.assertEqual(updated_entry.scan_det_plate, 'original_barcode_beadSet')

        kim.lpo.LimsPlateOrm.update_in_db = OG_LPO
    def test_copy_lxbs_to_project_directory(self):
        # SETUP MOCKS
        OG_glob = kim.glob.glob
        OG_shutil = kim.shutil.copyfile

        glob_results = ['glob1', 'glob2', 'glob3', 'glob4']
        kim.glob.glob = mock.Mock(return_value=glob_results)
        kim.shutil.copyfile = mock.Mock()

        # SETUP ARGS AND MAKE CALL
        fake_destination = 'destination_base_path'
        fake_scan = TestKim.create_scan_from_archive()
        kim.copy_lxbs_to_project_directory(fake_destination, fake_scan)

        # VALIDATE GLOB
        kim.glob.glob.assert_called_once
        glob_args = kim.glob.glob.call_args_list[0]
        self.assertEqual(glob_args, mock.call('fake_lxb_path/*.lxb'))

        # VALIDATE COPY
        copyfile_calls = kim.shutil.copyfile.call_args_list

        for (i, glob) in enumerate(glob_results):
            this_call = mock.call(glob_results[i], 'destination_base_path/lxb/' +glob_results[i])
            self.assertEqual(copyfile_calls[i], this_call)

        # RESET MOCKED FUNCTIONS
        kim.glob.glob = OG_glob
        kim.shutil.copyfile = OG_shutil


    # def test_copy_csv_to_project_directory(self):
    #     # SETUP MOCKS
    #     OG_shutil = kim.shutil.copyfile
    #     OG_path_exists = kim.os.path.exists
    #     OG_path_base = kim.os.path.basename
    #
    #     kim.shutil.copyfile = mock.Mock()
    #     kim.os.path.exists = mock.Mock(return_value=True)
    #     kim.os.path.basename = mock.Mock(return_value='path.csv')
    #
    #     # SETUP ARGS AND MAKE CALL
    #     fake_destination = "destination_lxb_dir"
    #     fake_scan = TestKim.create_scan_from_archive()
    #     kim.make_csv_in_lxb_directory(fake_destination, fake_scan)
    #
    #     # VALIDATE COPY
    #     call_made = kim.shutil.copyfile.call_args_list[0]
    #     expected_call = mock.call('fake_csv_path', 'destination_lxb_dir/path.csv')
    #     self.assertEqual(call_made, expected_call)
    #
    #     # RESET MOCKED FUNCTIONS
    #     kim.shutil.copyfile = OG_shutil
    #     kim.os.path.exists = OG_path_exists
    #     kim.os.path.basename = OG_path_base

if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    unittest.main()
