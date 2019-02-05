import logging

import broadinstitute.queue_manager.setup_logger as setup_logger


logger = logging.getLogger(setup_logger.LOGGER_NAME)


jobs_cols = ['queue', 'jenkins_id', 'flag']
data_cols =  ['plate_machine_barcode'] + jobs_cols
all_cols = data_cols + ['id']

# GET QUERIES
get_job_by_plate_machine_barcode_query = "SELECT {} FROM jobs WHERE plate_machine_barcode=%s".format(
    ", ".join(all_cols) )

get_job_by_queue_name_query = "SELECT {} FROM jobs WHERE queue=%s".format(", ".join(all_cols))

# SQL STATEMENTS
insert_jobs_statement = 'INSERT INTO jobs ({}) VALUES ({})'.format(
    ", ".join(data_cols), ", ".join("%s" for x in range(len(data_cols))) )

update_jobs_queue_statement = 'UPDATE jobs set queue=%s, jenkins_id=%s WHERE plate_machine_barcode=%s'

delete_job_statement = "DELETE FROM jobs where plate_machine_barcode=%s"

flag_plate_statement = 'UPDATE jobs set flag=%s where plate_machine_barcode=%s'

class JobsOrm(object):
    def __init__(self, plate_machine_barcode=None, queue=None, jenkins_id=None, flag=None, query_result_tuple=None):
        self.plate_machine_barcode = plate_machine_barcode
        self.queue = queue
        self.jenkins_id = jenkins_id
        self.flag = flag
        self.id = None

        if query_result_tuple is not None:
            self._assign_values_from_query(query_result_tuple)

    def __repr__(self):
        key_list = [x for x in self.__dict__.keys()]
        key_list.sort()
        ret_list = ["{}:{}".format(x, self.__dict__[x]) for x in key_list]

        return " ".join(ret_list)

    def __str__(self):
        return self.__repr__()

    def _assign_values_from_query(self, query_result_tuple):
        for n in range(len(all_cols)):
            setattr(self, all_cols[n], query_result_tuple[n])

    def create_entry_in_db(self, cursor):
        cursor.execute(insert_jobs_statement, (self.plate_machine_barcode, self.queue, self.jenkins_id, self.flag))
        logger.debug(cursor.statement)
        self.id = cursor.lastrowid
        return self.id

    def remove_entry_in_db(self, cursor):
        cursor.execute(delete_job_statement, (self.plate_machine_barcode,))
        logger.debug(cursor.statement)

    def update_jobs_queue(self, cursor):
        cursor.execute(update_jobs_queue_statement, (self.queue, self.jenkins_id, self.plate_machine_barcode))
        logger.debug(cursor.statement)

    def toggle_flag(self, cursor):
        if self.flag is None:
            self.flag = True
        else:
            self.flag = not self.flag
        cursor.execute(flag_plate_statement, (self.flag, self.plate_machine_barcode))
        logger.debug(cursor.statement)

def get_jobs_entry_by_plate_machine_barcode(cursor, plate_machine_barcode):
    cursor.execute(get_job_by_plate_machine_barcode_query, (plate_machine_barcode,))
    logger.debug(cursor.statement)
    r = cursor.fetchall()

    if len(r) == 0:
        return None
    else:
        return JobsOrm(query_result_tuple=r[0])

def get_plates_in_job_queue(cursor, queue):
    cursor.execute(get_job_by_queue_name_query, (queue,))
    logger.debug(cursor.statement)
    r = cursor.fetchall()
    if len(r) == 0:
        return None
    else:
        results = []
        for job in r:
            results.append(JobsOrm(query_result_tuple=job))

        return results

