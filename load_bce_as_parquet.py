import time

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from urllib.parse import quote_plus
import sqlalchemy as db
import re
import os

s3_bucket = os.getenv('S3_BUCKET')
chunksize = 10000000


def get_table_structure():
    # server = os.getenv('BCE_HOST')
    server = os.getenv('BCE_HOST')
    username = os.getenv('BCE_USER')
    pwd = os.getenv('BCE_PWD')
    database = os.getenv('BCE_DB')
    port = os.getenv('BCE_PORT')

    conn_url = f"mysql+mysqldb://" \
               f"{username}:{pwd}@{server}:{port}/{database}"
    engine = db.create_engine(conn_url)

    print('Connected to ', conn_url)

    sql_query = '''
    select
        *
    from publication
    '''

    create_parquet_file(
        sql_query,
        engine,
        'publication_table.parquet',
        'bce_to_s3',
        'publication',
        'bce_doe'
    )


def create_parquet_file(sql_query, engine, file_name, organisation_id, report_id, database_name):
    # store the results of the SQL query as dataframe

    start_time = time.process_time()
    iterator = pd.read_sql_query(
        sql_query,
        engine,
        coerce_float=True,
        chunksize=chunksize
    )
    end_time = time.process_time()

    print('Time in seconds for building the iterator with a chunksize of ',
          chunksize,
          (end_time - start_time),
          ' seconds.'
          )
    # loop over the iterable object created

    start_time = time.process_time()
    for i, df in enumerate(iterator):
        #     print(row.sum(axis = 0, skipna = True))
        # create a pyarrow table from the dataframe object
        table = pa.Table.from_pandas(df, preserve_index=False)
        # print('table created.')

        # create a parquet write object giving it an output file
        # pqwriter = pq.ParquetWriter(
        #     '/{}'.format(file_name),
        #     table.schema,
        #     compression='snappy'
        # )
        # pqwriter.write_table(table)
        df.to_parquet('publication_table.parquet')

    print('Successful write')
    end_time = time.process_time()
    print('Time in seconds for building the parquet file: ',
          (end_time - start_time),
          ' seconds.'
          )

    # engine.close()
    pattern = '[0-9]|\[ns\]|\[day\]'

    columns = []
    types = []
    for x in table.schema:
        columns.append(x.name)
        if 'null' not in re.sub(pattern, '', str(x.type)):
            types.append(re.sub(pattern, '', str(x.type)))
        else:
            types.append('string')
        print(x.name, re.sub(pattern, '', str(x.type)))

    s3_resource = boto3.resource('s3')
    try:
        bucket = s3_resource.Bucket(s3_bucket)
        objects = bucket.objects.filter(
            Prefix='{}/{}/'.format(organisation_id, str(report_id)))
        for obj in objects:
            obj.delete()
    except Exception as e:
        print("cannot delete s3 file", e)
    try:
        s3_resource.Bucket(s3_bucket).upload_file(
            Filename='/{}'.format(file_name)
            , Key='{}/{}/{}'.format(organisation_id, str(report_id), file_name))
    except Exception as e:
        print("cannot upload file to s3", e)

    os.remove('/{}'.format(file_name))

    s3_data_url = 's3://{}/{}/{}/'.format(s3_bucket, organisation_id, str(report_id))
    s3_url = 's3://' + s3_bucket + '/athena_query/'
    table_name = 'q{}'.format(report_id)

    engine = connect_with_athena(database_name, s3_url)

    create_athena_database(engine, database_name)
    drop_athena_table(engine, database_name, table_name)
    create_athena_table(engine, columns, types, database_name, table_name,
                        s3_data_url)

    engine.close()


def create_athena_database(engine, database_name):
    ddl = '''CREATE DATABASE IF NOT EXISTS {}'''.format(database_name)
    engine.execute(ddl)


def create_athena_table(engine, columns, types, database_name, table_name,
                        s3_url):
    columns_types = ''
    for column_, type_ in zip(columns, types):
        columns_types += '`{}` {},'.format(column_, type_)
    columns_types = columns_types[:-1]
    ddl = '''CREATE EXTERNAL TABLE IF NOT EXISTS {}.{} ({})
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde
    .ParquetHiveSerDe'
    WITH SERDEPROPERTIES (
    'serialization.format' = '1'
    ) LOCATION '{}'
    TBLPROPERTIES ('has_encrypted_data'='false');'''.format(database_name,
                                                            table_name,
                                                            columns_types,
                                                            s3_url)

    engine.execute(ddl)


def drop_athena_table(engine, database_name, table_name):
    ddl = '''DROP TABLE IF EXISTS {}.{}'''.format(database_name, table_name)

    engine.execute(ddl)


def connect_with_athena(database_name, s3_url):
    # conn_str = 'awsathena+rest://{aws_access_key_id}:{
    # aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/'\
    #         '{schema_name}?s3_staging_dir={s3_staging_dir}'
    conn_str = 'awsathena+rest://:@athena.{region_name}.amazonaws.com:443/' \
               '{schema_name}?s3_staging_dir={s3_staging_dir}'

    engine = create_engine(conn_str.format(
        region_name='ap-south-1',
        schema_name=database_name,
        s3_staging_dir=quote_plus(s3_url)))

    connection = engine.connect()

    return connection

if __name__ == "__main__":
    get_table_structure()
