import time

import pandas as pd

from bce_connection import BceConnection
from constants import STDOUT, CHUNKSIZE, table_name
from sql_queries import sql_query
from utils import export_parquet_to_s3, retrieve_chunks


class BceDateToParquet:
    def main(self):
        """Connect to BCE, retrieve data via an SQL query, and upload into S3 as parquet files

        :return: None
        """
        # Connect to BCE
        bce = BceConnection()
        engine = bce.bce_open_connection()

        # Store the results of the SQL query as dataframe
        s1 = time.process_time()
        chunks = retrieve_chunks(sql_query, engine)

        # Since chunksize is specified a concat is necessary
        df = pd.concat(list(chunks))
        STDOUT.write(f"Building iterator of chunksize {CHUNKSIZE} took (s): "
                     f"{time.process_time() - s1}\n")

        s2 = time.process_time()
        export_parquet_to_s3(df=df,
                             table_name=table_name)
        STDOUT.write(f'Exporting of parquet file (s): '
                     f'{time.process_time() - s2}\n')

        # Disconnect from BCE
        bce.bce_close_connection(engine)


if __name__ == "__main__":
    bce_data = BceDateToParquet()
    bce_data.main()
