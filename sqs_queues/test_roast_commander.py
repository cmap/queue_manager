import os
import unittest
import mock
import logging
import broadinstitute.queue_manager.setup_logger as setup_logger

import caldaia.utils.orm.lims_plate_orm as lpo
import caldaia.utils.config_tools as config_tools

import sqs_queues.roastcommander as rc

logger = logging.getLogger(setup_logger.LOGGER_NAME)

OG_LPO = rc.lpo.get_by_machine_barcode

test_barcode = 'machine_barcode'
test_det_plate = 'test_det_plate'
test_project = 'TEST'

class TestRoastCommander(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        rc.lpo.get_by_machine_barcode = mock.Mock()

    def setUp(self):
        rc.lpo.get_by_machine_barcode.return_value = TestRoastCommander.create_lpo()

    @classmethod
    def tearDownClass(cls):
        rc.lpo.get_by_machine_barcode = OG_LPO

    @staticmethod
    def build_args(machine_barcode):
        args = rc.build_parser().parse_args(['-machine_barcode', machine_barcode])
        config_tools.add_config_file_settings_to_args(args)
        return args


    @staticmethod
    def create_lpo():
        l = lpo.LimsPlateOrm()
        l.det_plate = test_det_plate
        l.project_code = test_project
        return l

    @staticmethod
    def common_roast_commander_setup(machine_barcode):
        cursor = mock.Mock()

        args = TestRoastCommander.build_args(machine_barcode)
        test = rc.RoastCommander(cursor, args.data_path, args.espresso_path, args.machine_barcode)
        return (test, args)

    def test__init__(self):
        (test_rc, args) = TestRoastCommander.common_roast_commander_setup(test_barcode)
        self.assertEqual(test_rc.base_path, args.data_path)
        self.assertEqual(test_rc.espresso_path, args.espresso_path)
        self.assertEqual(test_rc.plate, test_det_plate)


    def test__build_paths(self):
        (test_rc, args) = TestRoastCommander.common_roast_commander_setup(test_barcode)

        self.assertEqual(test_rc.project_directory, os.path.join(args.data_path, test_project))
        self.assertEqual(test_rc.map_dir_path, os.path.join(args.data_path, 'map_src'))
        self.assertEqual(test_rc.lxb_dir_path, os.path.join(args.data_path, test_project, 'lxb', test_det_plate))

    def test__build_command(self):
        (test_rc, args) = TestRoastCommander.common_roast_commander_setup(test_barcode)
        expected_command = """nohup matlab -nodesktop -nosplash -r "cd /cmap/tools/jenkins/job_repos/espresso/roast; roast('clean', true,
                            'plate', 'test_det_plate',
                            'plate_path', '/cmap/obelix/pod/custom/TEST/roast',
                            'map_path', '/cmap/obelix/pod/custom/map_src',
                            'raw_path', '/cmap/obelix/pod/custom/TEST/lxb/test_det_plate',
                            'parallel', true); quit" < /dev/null"""
        self.assertEqual(test_rc.command, expected_command)

    def test_make_job(self):
        args = TestRoastCommander.build_args(test_barcode)
        RC = rc.make_job(args)
        expected_fields = ["base_path", "espresso_path", "plate", "command", "lims_plate_orm", "project_directory",
                           "map_dir_path", "lxb_dir_path", "roast_dir_path"]
        for field in expected_fields:
            self.assertIsNotNone(getattr(RC, field))

if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    unittest.main()