from datetime import datetime

import awswrangler as wr
import pandas as pd

from constants import STDERR, STDOUT, S3_STAGING_BUCKET, S3_STAGING_PREFIX, CHUNKSIZE


def retrieve_chunks(
        sql_query: str,
        engine: "sqlalchemy.engine.Engine",
) -> list:
    """

    :param sql_query:
    :param engine:
    :return:
    """
    def chunk_generator():
        for chunk in pd.read_sql_query(sql_query, engine, coerce_float=True, chunksize=CHUNKSIZE):
            yield chunk

    return list(chunk_generator())


def export_parquet_to_s3(
        df: pd.DataFrame,
        bucket: str = S3_STAGING_BUCKET,
        prefix: str = S3_STAGING_PREFIX,
        table_name: str = None,
) -> None:
    """Export DF as parquet file to S3

    :param df: datframe
    :param bucket: dedicated bucket to use
    :param prefix: prefix (object) of the S3 bucket
    :param table_name: table name to retrieve data from
    :return: None
    """
    date = datetime.now().strftime("%Y/%m/%d")

    # writing df to Data Lake (S3 + Parquet + Glue Catalog)
    response = wr.s3.to_parquet(
        df=df,
        path=f"s3://{bucket}/{prefix}{table_name}/{date}",
        dataset=True,
        mode="overwrite"
    )

    STDOUT.write(f"Response from to_parquet: {response}\n")

    if response is None:
        STDERR.write(f"Response returned back None\n")



