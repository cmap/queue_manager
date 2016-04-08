import os
import sqlite3


creation_script_path = "database/update001_create_initial.sql"
insert_initial_espresso_prism_values_script_path = "database/prism_espresso_update002_insert_initial_values.sql"
insert_initial_psp_values_script_path = "database/psp_update002_insert_initial_values.sql"


def build(connection_string):
    f = open(creation_script_path)
    creation_script = f.read().strip()
    f.close()

    conn = sqlite3.connect(connection_string)
    conn.executescript(creation_script)
    conn.commit()

    return conn


def _apply_script(script_path, conn):
    f = open(script_path)
    script = f.read().strip()
    f.close()

    conn.executescript(script)


def insert_initial_espresso_prism_values(conn):
    _apply_script(insert_initial_espresso_prism_values_script_path, conn)


def insert_initial_psp_values(conn):
    _apply_script(insert_initial_psp_values_script_path, conn)
