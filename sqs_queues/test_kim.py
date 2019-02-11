import unittest
import mock
import logging

import caldaia.utils.orm.lims_plate_orm as lpo

import broadinstitute.queue_manager.setup_logger as setup_logger
import sqs_queues.kim as kim
import sqs_queues.queue_scan as scan

logger = logging.getLogger(setup_logger.LOGGER_NAME)


class TestKim(unittest.TestCase):

    @staticmethod
    def setup_args():
        args = kim.build_parser().parse_args(['--data_path', 'data/path','--hostname', 'babies', '--jenkins_id', '666'])
        # logger.debug(args)
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
    def create_scan_from_archive(machine_barcode):
        OG_LPO = scan.lpo.get_by_machine_barcode
        # OG_scan_os = scan.os.path.join
        OG_scan_get_num_lxb = scan.QueueScan.get_num_lxbs_scanned
        OG_scan_done = scan.QueueScan.check_scan_done

        scan.lpo.get_by_machine_barcode = mock.Mock(return_value=TestKim.create_lims_plate_orm())
        # scan.os.path.join = mock.Mock(return_value='fake_lxb_path')
        scan.QueueScan.get_num_lxbs_scanned = mock.Mock(return_value=384)
        scan.QueueScan.check_scan_done = mock.Mock(return_value=(True, 1))

        fake_scan = scan.QueueScan(None, "archive_path", "elapse_time", machine_barcode)

        scan.lpo.get_by_machine_barcode = OG_LPO
        # scan.os.path.join = OG_scan_os
        scan.QueueScan.get_num_lxbs_scanned = OG_scan_get_num_lxb
        scan.QueueScan.check_scan_done = OG_scan_done

        return fake_scan

    def test_sift_for_viable_jobs(self):
        OG_setup_dirs = kim.setup_project_directory_structure_if_needed
        OG_job_by_pmb = kim.jobs.get_jobs_entry_by_plate_machine_barcode

        fake_job = kim.jobs.JobsOrm(plate_machine_barcode="pmb1")
        kim.setup_project_directory_structure_if_needed = mock.Mock()
        kim.jobs.get_jobs_entry_by_plate_machine_barcode = mock.Mock(
            return_value =fake_job)
        cursor = mock.Mock()
        cursor.execute = mock.Mock()
        cursor.statement = mock.Mock()

        args = TestKim.setup_args()

        happy_scan = TestKim.create_scan_from_archive("machine_barcode")
        (prj_dir, lxb_dir, dev_flag) = kim.sift_for_viable_jobs(args, cursor, happy_scan)
        self.assertEqual(prj_dir, "data/path/PRJ")
        self.assertEqual(lxb_dir, "data/path/PRJ/lxb/det_plate")
        self.assertFalse(dev_flag)
        cursor.execute.assert_called_once
        job_entry = cursor.execute.call_args_list[0]
        expected_job_entry = mock.call(kim.jobs.update_jobs_queue_statement, ("kim", 666, "pmb1"))
        self.assertEqual(job_entry, expected_job_entry)

        cursor.execute.reset_mock()

        # DEV plate condition
        dev_scan = TestKim.create_scan_from_archive("DEV_plate")
        dev_scan.lims_plate_orm = None
        dev_scan.plate_search_name = "DEV_plate"
        (prj_dir, lxb_dir, dev_flag) = kim.sift_for_viable_jobs(args, cursor, dev_scan)
        self.assertEqual(prj_dir, "data/path/DEV")
        self.assertEqual(lxb_dir, "data/path/DEV/lxb/DEV_plate")
        self.assertTrue(dev_flag)
        cursor.execute.assert_not_called


        # no lims_plate_orm, not DEV plate condition
        unhappy_scan = TestKim.create_scan_from_archive("no_lpo")
        unhappy_scan.lims_plate_orm = None

        with self.assertRaises(SystemExit):
            kim.sift_for_viable_jobs(args, cursor, unhappy_scan)

        kim.setup_project_directory_structure_if_needed = OG_setup_dirs
        kim.jobs.get_jobs_entry_by_plate_machine_barcode = OG_job_by_pmb

    def test_update_or_create_job_entry(self):
        OG_job_by_pmb = kim.jobs.get_jobs_entry_by_plate_machine_barcode

        args = TestKim.setup_args()
        cursor = mock.Mock()
        cursor.execute = mock.Mock()
        cursor.statement = mock.Mock()

        # TEST UPDATE PATH
        mb = "pmb1"
        fake_job = kim.jobs.JobsOrm(plate_machine_barcode=mb)
        kim.jobs.get_jobs_entry_by_plate_machine_barcode = mock.Mock(
            return_value =fake_job)

        kim.update_or_create_job_entry(cursor, mb, args.jenkins_id)
        cursor.execute.assert_called_once
        job_entry = cursor.execute.call_args_list[0]
        expected_job_entry = mock.call(kim.jobs.update_jobs_queue_statement, ("kim", 666, "pmb1"))
        self.assertEqual(job_entry, expected_job_entry)

        kim.jobs.get_jobs_entry_by_plate_machine_barcode.reset_mock()
        cursor.reset_mock()
        cursor.execute.reset_mock()

        # TEST CREATE PATH
        kim.jobs.get_jobs_entry_by_plate_machine_barcode.return_value = None

        kim.update_or_create_job_entry(cursor, mb, args.jenkins_id)
        cursor.execute.assert_called_once()
        job_entry = cursor.execute.call_args_list[0]
        expected_job_entry = mock.call(kim.jobs.insert_jobs_statement, (mb, "kim", 666, None))
        self.assertEqual(job_entry, expected_job_entry)

        kim.jobs.get_jobs_entry_by_plate_machine_barcode = OG_job_by_pmb


    def test_setup_project_directory_structure_if_needed(self):
        OG_p_exists = kim.os.path.exists
        OG_mkdir = kim.os.mkdir

        kim.os.path.exists = mock.Mock(return_value=True)
        kim.os.mkdir = mock.Mock()

        kim.setup_project_directory_structure_if_needed("this_path_exists")
        kim.os.mkdir.assert_not_called

        kim.os.path.exists.return_value = False

        kim.setup_project_directory_structure_if_needed("this_path_dne")
        kim.os.mkdir.assert_called

        mkdir_calls = kim.os.mkdir.call_args_list
        expected_calls = [mock.call("this_path_dne"), mock.call("this_path_dne/lxb"), mock.call("this_path_dne/map_src"),
                          mock.call("this_path_dne/maps"), mock.call("this_path_dne/roast"), mock.call("this_path_dne/brew"),
                          mock.call("this_path_dne/cup")]
        self.assertEqual(mkdir_calls, expected_calls)

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
        fake_destination = 'destination_base_path/lxb'
        fake_scan = TestKim.create_scan_from_archive("machine_barcode")
        kim.copy_lxbs_to_project_directory(fake_destination, fake_scan)

        # VALIDATE GLOB
        kim.glob.glob.assert_called_once
        glob_args = kim.glob.glob.call_args_list[0]
        self.assertEqual(glob_args, mock.call('archive_path/lxb/det_plate*/*.lxb'))

        # VALIDATE COPY
        copyfile_calls = kim.shutil.copyfile.call_args_list

        for (i, glob) in enumerate(glob_results):
            this_call = mock.call(glob_results[i], 'destination_base_path/lxb/' +glob_results[i])
            self.assertEqual(copyfile_calls[i], this_call)

        # RESET MOCKED FUNCTIONS
        kim.glob.glob = OG_glob
        kim.shutil.copyfile = OG_shutil

    def test_make_jcsv_in_lxb_directory(self):
        # SET UP MOCKS
        OG_cmd = kim.os.system
        kim.os.system = mock.Mock(return_value=True)

        # SETUP ARGS AND MAKE CALL
        (lxb2jcsv_path, destination_lxb_dir) = ("lxb2jcsv/path", "destination/path")
        outfile = kim.make_jcsv_in_lxb_directory(lxb2jcsv_path, destination_lxb_dir, "det_plate")

        # VALIDATE OUTPUT
        expected_outfile = "destination/path/det_plate"
        self.assertEqual(outfile, expected_outfile)

        # VALIDATE CMD
        kim.os.system.assert_called_once
        mk_jcsv_cmd = kim.os.system.call_args_list[0]
        expected_cmd = mock.call("lxb2jcsv/path -i destination/path -o " + expected_outfile)
        self.assertEqual(mk_jcsv_cmd, expected_cmd)

        kim.os.system = OG_cmd
    def test_make_lims_database_updates(self):
        cursor = mock.Mock()
        cursor.execute = mock.Mock()

        updated_entry = kim.make_lims_database_updates(cursor, TestKim.create_lims_plate_orm())

        cursor.execute.assert_called_once
        self.assertEqual(updated_entry.rna_plate, 'pertPlate_cellId_pertTime_repNum')
        self.assertEqual(updated_entry.det_plate, 'pertPlate_cellId_pertTime_repNum_beadSet')
        self.assertEqual(updated_entry.scan_det_plate, 'original_barcode_beadSet')



if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    unittest.main()
