import pandas as pd
from sqlalchemy import create_engine

import os
from os import getenv, listdir
from os.path import isfile, join
from dotenv import load_dotenv

#------------------------------------------------------------------------
# ENV
#------------------------------------------------------------------------
load_dotenv(dotenv_path = 'db_info.env')

DB_HOST = getenv('DB_HOST', None)
DB_PORT = getenv('DB_PORT', 5432)
DB_USER = getenv('DB_USER', None)
DB_PASS = getenv('DB_PASS', None)
DB_NAME = getenv('DB_NAME', None)


engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

file_dir = "./synthea_cdm_csv"
file_names = [f for f in listdir(file_dir) if isfile(join(file_dir, f))]

for file_name in file_names:
    df = pd.read_csv(file_dir + f"/{file_name}")
    df.columns = [c.lower() for c in df.columns]
    print(f"INSERT {file_name} ING... ")
    df.to_sql(f"{file_name.split('.')[0]}", engine)

print("COMPLETE")