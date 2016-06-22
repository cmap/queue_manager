import sqlite3
import ConfigParser


config_database_section = "Database"


def build(connection_string, config_filepath):
    cp = ConfigParser.RawConfigParser()
    cp.read(config_filepath)

    f = open(cp.get(config_database_section, "creation_script_path"))

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


def insert_initial_espresso_prism_values(conn, config_filepath):
    cp = ConfigParser.RawConfigParser()
    cp.read(config_filepath)

    _apply_script(cp.get(config_database_section, "prism_espresso_update002_script_path"), conn)
    _apply_script(cp.get(config_database_section, "prism_espresso_update003_script_path"), conn)


def insert_initial_psp_values(conn, config_filepath):
    cp = ConfigParser.RawConfigParser()
    cp.read(config_filepath)

    _apply_script(cp.get(config_database_section, "insert_initial_psp_values_script_path"), conn)
