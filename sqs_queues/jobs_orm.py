

jobs_cols = ['queue', 'flag', 'jenkins_id']
data_cols = ['plate_machine_barcode'] + jobs_cols
all_cols = ['id', 'last_updated', 'created'] + data_cols

# GET QUERIES
get_by_plate_machine_barcode = "SELECT {} FROM jobs WHERE plate_machine_barcode=%s".format(
    ", ".join(all_cols) )

get_by_job_queue = "SELECT {} FROM jobs WHERE queue=%s".format(", ".join(all_cols))

# SQL STATEMENTS
insert_jobs_statement = 'INSERT INTO jobs ({}) VALUES ({})'.format(
    ", ".join(data_cols), ", ".join("%s" for x in range(len(data_cols))) )

update_jobs_queue_statement = 'UPDATE jobs set queue=%s, jenkins_id=%s WHERE plate_machine_barcode=%s'

flag_plate_statement = 'UPDATE jobs set flag=%s where plate_machine_barcode=%s'

class JobsOrm(object):
    def __init__(self, plate_machine_barcode=None, queue=None, flag=None, jenkins_id=None, query_result_list=None):
        self.plate_machine_barcode = plate_machine_barcode
        self.queue = queue
        self.flag = flag
        self.jenkins_id = jenkins_id

        if query_result_list is not None:
            self._assign_values_from_query(query_result_list)

    def _assign_values_from_query(self, query_result_list):
        for n in range(len(all_cols)):
            setattr(self, all_cols[n], query_result_list[n])

    def create_entry_in_db(self, cursor):
        cursor.execute(insert_jobs_statement, (self.plate_machine_barcode, self.queue, self.flag, self.jenkins_id))

        self.id = cursor.lastrowid

    def update_jobs_queue(self, cursor):
        cursor.execute(update_jobs_queue_statement, (self.queue, self.jenkins_id, self.plate_machine_barcode))

    def toggle_flag(self, cursor):
        if self.flag is None:
            self.flag = True
        else:
            self.flag = not self.flag
        cursor.execute(flag_plate_statement, (self.flag, self.plate_machine_barcode))

def get_jobs_entry_by_plate_machine_barcode(cursor, plate_machine_barcode):
    cursor.execute(get_by_plate_machine_barcode, (plate_machine_barcode,))
    r = cursor.fetchall()

    if len(r) == 0:
        return None
    else:
        return JobsOrm(query_result_list=r[0])

def get_plates_in_job_queue(cursor, queue):
    cursor.execute(get_by_job_queue, (queue,))
    results = []
    for r in cursor:
        results.append(JobsOrm(query_result_list=r))

    return results

