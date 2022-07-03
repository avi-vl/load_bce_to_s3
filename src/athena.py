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