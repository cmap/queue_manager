import os
import sqlite3


creation_script_path = "../database/update001_create_initial.sql"


def build(connection_string):
    f = open(creation_script_path)
    creation_script = f.read().strip()
    f.close()

    conn = sqlite3.connect(connection_string)
    conn.executescript(creation_script)
    conn.commit()

    return conn
