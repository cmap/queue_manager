import unittest
import logging

import caldaia.utils
import caldaia.utils.mysql_utils as mysql_utils
import broadinstitute.queue_manager.setup_logger as setup_logger
import sqs_queues.jobs_orm as jobs

logger = logging.getLogger(setup_logger.LOGGER_NAME)

db = None

(test_pmb, test_queue, test_jenkins_id, test_flag) = (24, 'test', 1234, False)


def setup_update_test_job(db):
    cursor = db.cursor()
    r = jobs.get_plates_in_job_queue(cursor, 'test')
    if r is None:
        r = jobs.JobsOrm(plate_machine_barcode=test_pmb, queue=test_queue,
                         jenkins_id=test_jenkins_id, flag=test_flag)
        r.create_entry_in_db(cursor)
        db.commit()
        cursor.close()
        return r
    else:
        cursor.close()
        return r[0]


class TestJobsOrm(unittest.TestCase):

    @staticmethod
    def create_test_jobs_orm(pmb=23, queue='yeezy', j_id=1234, flag=True):
        j = jobs.JobsOrm(plate_machine_barcode=pmb, queue=queue, jenkins_id=j_id, flag=flag)
        return j

    def test_query_strings(self):
        logger.debug("get_by_plate_machine_barcode_query: {}".format(jobs.get_job_by_plate_machine_barcode_query))
        logger.debug("get_by_job_queue_query: {}".format(jobs.get_job_by_queue_name_query))

    def test__init__(self):
        job = jobs.JobsOrm()
        assert job
        for attribute in [job.queue, job.plate_machine_barcode, job.flag, job.jenkins_id]:
            self.assertIsNone(attribute)

        job = jobs.JobsOrm(query_result_tuple=range(5))
        assert job
        for attribute in [job.queue, job.plate_machine_barcode, job.flag, job.jenkins_id, job.id]:
            self.assertIsNotNone(attribute)

    def test__assign_values_from_query(self):
        job = jobs.JobsOrm()
        job._assign_values_from_query(range(5))
        for attribute in [job.queue, job.plate_machine_barcode, job.flag, job.jenkins_id, job.id]:
            self.assertIsNotNone(attribute)

    def test_create_entry_in_db(self):
        cursor = db.cursor()

        test_job_orm = TestJobsOrm.create_test_jobs_orm()
        id = test_job_orm.create_entry_in_db(cursor)
        self.assertIsNotNone(id)

        cursor.execute("select * from jobs where id=%s", (id,))
        r = cursor.fetchall()[0]
        self.assertIsNotNone(r)

        expectation_list = (id, test_job_orm.plate_machine_barcode, test_job_orm.queue,
                            test_job_orm.jenkins_id, test_job_orm.flag)
        for i, value in enumerate(r):
            # don't check timestamp values
            if i not in [5,6]:
                self.assertEqual(r[i], expectation_list[i])

        # timestamps should be equal on creation
        self.assertEqual(r[5], r[6])
        db.rollback()
        cursor.close()

    def test_remove_entry_in_db(self):
        cursor = db.cursor()
        test_job_orm = TestJobsOrm.create_test_jobs_orm()
        id = test_job_orm.create_entry_in_db(cursor)
        self.assertIsNotNone(id)

        cursor.execute("select * from jobs where id=%s", (id,))
        r = cursor.fetchall()[0]
        self.assertIsNotNone(r)

        test_job_orm.remove_entry_in_db(cursor)

        cursor.execute("select * from jobs where id=%s", (id,))
        r = cursor.fetchall()
        self.assertEqual(len(r), 0)


    def test_update_jobs_queue(self):
        cursor = db.cursor()

        (new_queue, new_jenkins_id) = ("kim", 9876)

        update_test_job.queue = new_queue
        update_test_job.jenkins_id = new_jenkins_id

        update_test_job.update_jobs_queue(cursor)

        cursor.execute("select * from jobs where id=%s", (update_test_job.id,))
        r = cursor.fetchall()[0]
        self.assertIsNotNone(r)

        expectations = (update_test_job.id, update_test_job.plate_machine_barcode,
                            new_queue, new_jenkins_id, update_test_job.flag)

        for i, value in enumerate(r):
            # don't check timestamp values
            if i not in [5, 6]:
                self.assertEqual(r[i], expectations[i])

        # make sure last_updated timestamp was modified
        self.assertNotEqual(r[5], r[6])

        db.rollback()
        cursor.close()

    def test_toggle_flag(self):
        cursor = db.cursor()
        test_job_orm = TestJobsOrm.create_test_jobs_orm()
        id = test_job_orm.create_entry_in_db(cursor)
        self.assertIsNotNone(id)

        cursor.execute("select flag from jobs where id=%s", (id,))
        pre_toggle_flag_value = cursor.fetchall()[0]

        test_job_orm.toggle_flag(cursor)

        cursor.execute("select flag from jobs where id=%s", (id,))
        post_toggle_flag_value = cursor.fetchall()[0]
        self.assertIsNotNone(post_toggle_flag_value)
        self.assertEqual(pre_toggle_flag_value[0], not(post_toggle_flag_value[0]))

        db.rollback()

        test_job_orm2 = TestJobsOrm.create_test_jobs_orm(flag=None)
        id = test_job_orm2.create_entry_in_db(cursor)

        cursor.execute("select flag from jobs where id=%s", (id,))
        pre_toggle_flag_value = cursor.fetchall()[0][0]
        self.assertIsNone(pre_toggle_flag_value)

        test_job_orm.toggle_flag(cursor)

        cursor.execute("select flag from jobs where id=%s", (id,))
        post_toggle_flag_value = cursor.fetchall()[0][0]
        self.assertIsNotNone(post_toggle_flag_value)
        self.assertTrue(post_toggle_flag_value == 1)

        db.rollback()
        cursor.close()

    def test_get_jobs_entry_by_plate_machine_barcode(self):
        cursor = db.cursor()

        test_job_orm = jobs.get_jobs_entry_by_plate_machine_barcode(cursor, test_pmb)
        self.assertIsNotNone(test_job_orm)

        self.assertEqual(test_job_orm.plate_machine_barcode, test_pmb)
        self.assertEqual(test_job_orm.queue, test_queue)
        self.assertEqual(test_job_orm.jenkins_id, test_jenkins_id)
        self.assertEqual(test_job_orm.flag, test_flag)

        cursor.close()

    def test_get_plates_in_job_queue(self):
        cursor = db.cursor()

        queue_name = "testing"
        jobslist = [{"pmb":25, "j_id": 666},{"pmb": 26,"j_id": 111}]
        job_orm_list = []

        for i, job in enumerate(jobslist):
            job_orm = TestJobsOrm.create_test_jobs_orm(pmb=job["pmb"], queue=queue_name, j_id=job["j_id"])
            job_orm.create_entry_in_db(cursor)
            job_orm_list.append(job_orm)
            db.commit()

        r = jobs.get_plates_in_job_queue(cursor, queue_name)
        self.assertIsNotNone(r)
        self.assertEqual(len(r), len(jobslist))

        for job in job_orm_list:
            job.remove_entry_in_db(cursor)

        cursor.close()

if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    db = mysql_utils.DB(config_filepath=caldaia.utils.default_config_filepath, config_section="test").db

    update_test_job = setup_update_test_job(db)
    # validate_database_setup(db.cursor(), test_id)

    unittest.main()

    db.close()