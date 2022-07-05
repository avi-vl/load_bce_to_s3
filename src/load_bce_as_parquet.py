import time

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from urllib.parse import quote_plus
import sqlalchemy as db
import re
import os
import dask.dataframe as da

from athena import connect_with_athena, create_athena_table, \
    drop_athena_table, create_athena_database
from constants import STDOUT, STDERR, CHUNKSIZE, REGEX_PATERN, table_name
from sql_queries import sql_query
from tables import query_schema
from utils import convert_types, export_parquet_to_s3



def main():
    server = os.getenv('BCE_HOST')
    username = os.getenv('BCE_USER')
    pwd = os.getenv('BCE_PWD')
    database = os.getenv('BCE_DB')
    port = os.getenv('BCE_PORT')

    conn_url = f"mysql+mysqldb://" \
               f"{username}:{pwd}@{server}:{port}/{database}"
    engine = db.create_engine(conn_url, pool_size=20)

    STDOUT.write(f"Connected to: {conn_url}\n")

    create_parquet_file(
        sql_query,
        engine,
        f'{table_name}_table.parquet',
        'bce_to_s3',
        table_name,
        'bce_doe'
    )


def create_parquet_file(sql_query, engine, file_name, organisation_id,
                        report_id, database_name):
    # store the results of the SQL query as dataframe
    start_time_iterator = time.process_time()

    # since chunksize is specified a concat is necessary
    chunks = []
    for chunk in pd.read_sql_query(sql_query, engine, coerce_float=True, chunksize=CHUNKSIZE):
        chunks.append(chunk)
    df = pd.concat(list(chunks))

    STDOUT.write(f"Building iterator of chunksize {CHUNKSIZE} took (s): "
                 f"{time.process_time() - start_time_iterator}\n")

    schema = query_schema.get(report_id)[0]
    pyarrow_schema = query_schema.get(report_id)[1]

    # TODO: only to determine the dtypes of the df
    result = df.dtypes
    print(result)

    df = convert_types(df, df_schema=schema)

    start_time_parquet = time.process_time()
    ddf = da.from_pandas(df, chunksize=5000000)

    export_parquet_to_s3(df=ddf,
                         pyarrow_schema=pyarrow_schema,
                         table_name=report_id)

    STDOUT.write(f'Parquet file built in (s): '
                 f'{time.process_time() - start_time_parquet}')


def upload_parquet_to_s3():
    """

    :return:
    """
    columns = []
    types = []
    for x in table.schema:
        columns.append(x.name)
        if 'null' not in re.sub(REGEX_PATERN, '', str(x.type)):
            types.append(re.sub(REGEX_PATERN, '', str(x.type)))
        else:
            types.append('string')
        STDOUT.write(x.name, re.sub(REGEX_PATERN, '', str(x.type)))

    s3_resource = boto3.resource('s3')
    try:
        bucket = s3_resource.Bucket(S3_BUCKET)
        objects = bucket.objects.filter(
            Prefix='{}/{}/'.format(organisation_id, str(report_id)))
        for obj in objects:
            obj.delete()
    except Exception as e:
        STDERR.write(f"Cannot delete s3 file: {e}")
    try:
        s3_resource.Bucket(S3_BUCKET).upload_file(
            Filename='/{}'.format(file_name)
            , Key='{}/{}/{}'.format(organisation_id, str(report_id), file_name))
    except Exception as e:
        STDERR.write(f"cannot upload file to s3: {e}")

    os.remove('/{}'.format(file_name))

    s3_data_url = 's3://{}/{}/{}/'.format(S3_BUCKET, organisation_id,
                                          str(report_id))
    s3_url = 's3://' + S3_BUCKET + '/athena_query/'
    table_name = 'q{}'.format(report_id)

    engine = connect_with_athena(database_name, s3_url)

    create_athena_database(engine, database_name)
    drop_athena_table(engine, database_name, table_name)
    create_athena_table(engine, columns, types, database_name, table_name,
                        s3_data_url)

    engine.close()


if __name__ == "__main__":
    main()
