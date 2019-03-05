import os
import unittest
import mock
import logging

import caldaia.utils.orm.lims_plate_orm as lpo
import caldaia.utils.config_tools as config_tools

import broadinstitute.queue_manager.setup_logger as setup_logger
import sqs_queues.kim as kim

logger = logging.getLogger(setup_logger.LOGGER_NAME)
OG_LPO_get = kim.si.lpo.get_by_machine_barcode
OG_scan_num_lxbs = kim.Kim.get_num_lxbs_scanned
OG_scan_last_lxb = kim.Kim.check_last_lxb_addition

test_barcode = 'test_barcode'

class TestKim(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        kim.si.lpo.get_by_machine_barcode = mock.Mock()
        kim.Kim.get_num_lxbs_scanned = mock.Mock()
        kim.Kim.check_last_lxb_addition = mock.Mock()

    def setUp(self):
        kim.si.lpo.get_by_machine_barcode.return_value = TestKim.create_lims_plate_orm()

    @classmethod
    def tearDownClass(cls):
        kim.si.lpo.get_by_machine_barcode = OG_LPO_get
        kim.Kim.get_num_lxbs_scanned = OG_scan_num_lxbs
        kim.Kim.check_last_lxb_addition = OG_scan_last_lxb

    @staticmethod
    def build_args(machine_barcode):
        args = kim.build_parser().parse_args(['-machine_barcode', machine_barcode])
        config_tools.add_config_file_settings_to_args(args)
        return args

    @staticmethod
    def create_lims_plate_orm():

        l = lpo.LimsPlateOrm()

        l.project_code = 'PRJ'
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
    def common_setup_kim(machine_barcode):
        args = TestKim.build_args(machine_barcode)
        cursor = mock.Mock()

        test = kim.Kim(cursor, args.archive_path, args.data_path, args.lxb2jcsv_path, args.machine_barcode)
        print test
        return (test, args)

    def test__init__(self):
        (test_kim, args) = TestKim.common_setup_kim('machine_barcode')
        self.assertEqual(test_kim.plate_search_name, 'det_plate')
        self.assertEqual(test_kim.base_data_path, args.data_path)
        self.assertEqual(test_kim.lxb2jcsv_path, args.lxb2jcsv_path)
        self.assertEqual(test_kim.is_dev, False)
        self.assertEqual(test_kim.destination_project_dir, '/cmap/obelix/pod/custom/PRJ')
        self.assertEqual(test_kim.destination_lxb_dir, '/cmap/obelix/pod/custom/PRJ/lxb/det_plate')

    def test_set_destination_dirs(self):
        # HAPPY CONDITION, LPO RETURNED
        (test_kim, args) = TestKim.common_setup_kim('machine_barcode')
        test_kim.base_data_path = 'base_data_path'
        (prj_dir, lxb_dir) = test_kim.set_destination_dirs()

        self.assertEqual(prj_dir, 'base_data_path/PRJ')
        self.assertEqual(lxb_dir, 'base_data_path/PRJ/lxb/det_plate')


        # UNHAPPY CONDITION, LPO IS NONE
        kim.si.lpo.get_by_machine_barcode.return_value = None
        (test_kim, args) = TestKim.common_setup_kim('DEV_plate')
        test_kim.base_data_path = 'base_data_path'
        (prj_dir, lxb_dir) = test_kim.set_destination_dirs()

        self.assertEqual(prj_dir, "base_data_path/DEV")
        self.assertEqual(lxb_dir, "base_data_path/DEV/lxb/DEV_plate")

    def test_check_dev(self):
        kim.si.lpo.get_by_machine_barcode.return_value = None
        (test_kim, args) = TestKim.common_setup_kim('DEV_plate')
        self.assertEqual(test_kim.plate_search_name, 'DEV_plate')
        self.assertTrue(test_kim.is_dev)

        is_dev = test_kim.check_for_dev()
        self.assertTrue(is_dev)

    def test_setup_project_directory_structure_if_needed(self):
        # SET UP FOR TEAR DOWN
        OG_p_exists = kim.os.path.exists
        OG_mkdir = kim.os.mkdir

        # SET UP MOCKS
        kim.os.path.exists = mock.Mock(return_value=True)
        kim.os.mkdir = mock.Mock()

        # HAPPY CONDITION
        (test_kim, args) = TestKim.common_setup_kim('DEV_plate')
        setup_dir = test_kim.setup_project_directory_structure_if_needed()

        self.assertFalse(setup_dir)
        kim.os.mkdir.assert_not_called

        # UNHAPPY CONDITION
        kim.os.path.exists.return_value = False
        test_kim.destination_project_dir = "this_path_dne"
        setup_dir = test_kim.setup_project_directory_structure_if_needed()

        self.assertTrue(setup_dir)
        kim.os.mkdir.assert_called
        mkdir_calls = kim.os.mkdir.call_args_list
        expected_calls = [mock.call("this_path_dne"), mock.call("this_path_dne/lxb"), mock.call("this_path_dne/map_src"),
                          mock.call("this_path_dne/maps"), mock.call("this_path_dne/roast"), mock.call("this_path_dne/brew"),
                          mock.call("this_path_dne/cup")]
        self.assertEqual(mkdir_calls, expected_calls)

        # TEAR DOWN
        kim.os.path.exists = OG_p_exists
        kim.os.mkdir = OG_mkdir


    def test_copy_lxbs_to_project_directory(self):
        # SETUP MOCKS
        OG_glob = kim.glob.glob
        OG_shutil = kim.shutil.copyfile

        glob_results = ['glob1', 'glob2', 'glob3', 'glob4']
        kim.glob.glob = mock.Mock(return_value=glob_results)
        kim.shutil.copyfile = mock.Mock()

        # SETUP ARGS AND MAKE CALL
        (test_kim, args) = TestKim.common_setup_kim('DEV_plate')
        result = test_kim.copy_lxbs_to_project_directory()

        # VALIDATE GLOB
        self.assertTrue(result)
        kim.glob.glob.assert_called_once
        glob_args = kim.glob.glob.call_args_list[0]
        self.assertEqual(glob_args, mock.call(os.path.join(args.archive_path,'lxb/det_plate*/*.lxb')))

        # VALIDATE COPY
        copyfile_calls = kim.shutil.copyfile.call_args_list

        for (i, glob) in enumerate(glob_results):
            this_call = mock.call(glob_results[i],os.path.join(test_kim.destination_lxb_dir, glob_results[i]))
            self.assertEqual(copyfile_calls[i], this_call)

        # RESET MOCKED FUNCTIONS
        kim.glob.glob = OG_glob
        kim.shutil.copyfile = OG_shutil

    def test_make_jcsv_in_lxb_directory(self):
        # SET UP MOCKS
        OG_cmd = kim.os.system
        kim.os.system = mock.Mock(return_value=True)

        # SETUP ARGS AND MAKE CALL
        (test_kim, args) = TestKim.common_setup_kim('machine_barcode')
        outfile = test_kim.make_jcsv_in_lxb_directory()

        # VALIDATE OUTPUT
        expected_outfile = os.path.join(test_kim.destination_lxb_dir,"det_plate"+".jcsv")
        self.assertEqual(outfile, expected_outfile)

        # VALIDATE CMD
        kim.os.system.assert_called_once
        mk_jcsv_cmd = kim.os.system.call_args_list[0]
        expected_cmd = mock.call("{} -i {} -o {}".format(args.lxb2jcsv_path, test_kim.destination_lxb_dir, expected_outfile))
        self.assertEqual(mk_jcsv_cmd, expected_cmd)

        kim.os.system = OG_cmd
    def test_make_lims_database_updates(self):
        cursor = mock.Mock()
        cursor.execute = mock.Mock()

        (test_kim, args) = TestKim.common_setup_kim('machine_barcode')
        test_kim.cursor = cursor
        updated_entry = test_kim.make_lims_database_updates()

        cursor.execute.assert_called_once
        self.assertEqual(updated_entry.rna_plate, 'pertPlate_cellId_pertTime_repNum')
        self.assertEqual(updated_entry.det_plate, 'pertPlate_cellId_pertTime_repNum_beadSet')
        self.assertEqual(updated_entry.scan_det_plate, 'original_barcode_beadSet')

        #todo reminder
        cursor_call = cursor.execute.call_args_list[0]
        logger.warning("add test for cursor.statement {}".format(cursor_call))

    def test_make_job(self):
        args = TestKim.build_args(test_barcode)
        Kim = kim.make_job(args)
        expected_fields = ["cursor", "archive_path", "lxb_path", "num_lxbs_scanned", "lims_plate_orm",
                           "plate_search_name", "base_data_path", "lxb2jcsv_path", "is_dev",
                           "destination_project_dir", "destination_lxb_dir"]


        for field in expected_fields:
            self.assertIsNotNone(getattr(Kim, field))

    def test_execute_command(self):
        pass


if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    unittest.main()
