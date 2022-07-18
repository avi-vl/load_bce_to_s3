import time

import pandas as pd

from bce_connection import bce_connection
from constants import STDOUT, CHUNKSIZE, table_name
from sql_queries import sql_query
from utils import export_parquet_to_s3



def main():
    engine = bce_connection()

    create_parquet_file(
        sql_query,
        engine,
        table_name
    )


def create_parquet_file(sql_query, engine, report_id):
    # store the results of the SQL query as dataframe
    start_time_iterator = time.process_time()

    # since chunksize is specified a concat is necessary

    chunks = []
    for chunk in pd.read_sql_query(sql_query, engine, coerce_float=True, chunksize=CHUNKSIZE):
        chunks.append(chunk)
    df = pd.concat(list(chunks))

    STDOUT.write(f"Building iterator of chunksize {CHUNKSIZE} took (s): "
                 f"{time.process_time() - start_time_iterator}\n")

    start_time_parquet = time.process_time()
    export_parquet_to_s3(df=df,
                         table_name=report_id)
    STDOUT.write(f'Parquet file built in (s): '
                 f'{time.process_time() - start_time_parquet}\n')


if __name__ == "__main__":
    main()
