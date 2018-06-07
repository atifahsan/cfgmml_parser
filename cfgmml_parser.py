import os
import re
import fnmatch
import sqlite3
import pandas as pd
from collections import OrderedDict

PATTERN = "CFGMML*.txt"


def cfgmml_parser():
    cfgmml_folder, db_folder = init_app()
    db = sqlite3.connect(os.path.join(db_folder, 'dump.db'))
    files = get_filelist(cfgmml_folder)
    for file in files:
        data_dict = parse_file(file)
        store_in_db(data_dict, db)
    print('Completed successfully')

def init_app():
    root_folder = os.path.abspath(os.path.dirname(__file__))
    cfgmml_folder = os.path.join(root_folder, 'cfgmml')
    db_folder = os.path.join(root_folder, 'database')
    os.makedirs(db_folder, exist_ok=True)
    return cfgmml_folder, db_folder

def store_in_db(data_dict, db):
    """Writes each MML parameters as seperate table in SQLite DB
    Arguments:
        data_dict: dict -- dict of mml and parameters
        db: sqlite3.Connection -- DB to store parameters in table format
    """

    for tablename, v in data_dict.items():
        chunk = pd.DataFrame(v, columns=v[0].keys())
        while True:
            try:
                chunk.to_sql(tablename, db, if_exists="append", index=False)
                break
            except sqlite3.OperationalError as e:
                col_add = ('"' + re.search(r"no column named\s*([^#]+|\S+)", str(e)).group(1) + '"')
                query = 'ALTER TABLE "{}" ADD COLUMN {} TEXT;'.format(tablename, col_add)
                db.execute(query)

def parse_file(file):
    """Parses each line of CFGMML and store it in a 2 dimensional dict
    Arguments:
        file: file object -- CFGMML file
    Returns:
        dict -- 2 dimensional dict {mml: [{line1}, {line2}, ... {lineN}]}
    """
    print(f'Parsing {file}')
    src = 'SRC' # initalizing src incase no value is found in CFGMML file this value will be used as dummy value
    data_dict = OrderedDict()
    with open(file, "r") as f:
        for line in f:
            if line.startswith('//System BSCID:'):
                src = line.strip().split(':')[1]
            if not line.startswith("//"):
                try:
                    mml, cmd = line[:-2].split(":", 1)
                    cmd = OrderedDict(x.strip().split("=") for x in cmd.split(","))
                    cmd.update({'SRNC': src})
                    cmd.move_to_end('SRNC', last=False)
                    if mml not in data_dict:
                        data_dict[mml] = [cmd]
                    else:
                        data_dict[mml].append(cmd)
                except:
                    pass
    return data_dict


def get_filelist(path):
    """Returns list of CFGMML files in given folder
    Arguments:
        path: str -- Path of folder containing CFGMML files
    Returns:
        list -- List of CFGMML files in the folder
    """

    file_list = []
    for path, dirlist, filelist in os.walk(path):
        for name in fnmatch.filter(filelist, PATTERN):
            file_list.append(os.path.join(path, name))
    return file_list


if __name__ == "__main__":
    cfgmml_parser()
