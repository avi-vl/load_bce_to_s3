from datetime import datetime

import awswrangler as wr
import pandas as pd

from constants import STDERR, S3_STAGING_BUCKET, S3_STAGING_PREFIX, CHUNKSIZE


def retrieve_chunks(sql_query, engine):
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
    """

    :param df:
    :param bucket:
    :param prefix:
    :param table_name:
    :return:
    """
    date = datetime.now().strftime("%Y/%m/%d")

    # writing df to Data Lake (S3 + Parquet + Glue Catalog)
    response = wr.s3.to_parquet(
        df=df,
        path=f"s3://{bucket}/{prefix}{table_name}/{date}",
        dataset=True,
        mode="overwrite"
    )

    if response is not None:
        STDERR.write(f"Response from to_parquet: {response}\n")



