import os
import sqlite3


creation_script_path = "../database/update001_create_initial.sql"
insert_initial_prism_values_script_path = "../database/prism_espresso_update002_insert_initial_values.sql"


def build(connection_string):
    f = open(creation_script_path)
    creation_script = f.read().strip()
    f.close()

    conn = sqlite3.connect(connection_string)
    conn.executescript(creation_script)
    conn.commit()

    return conn


def insert_initial_prism_values(conn):
    f = open(insert_initial_prism_values_script_path)
    insert_script = f.read().strip()
    f.close()

    conn.executescript(insert_script)
