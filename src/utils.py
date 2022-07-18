from datetime import datetime

import awswrangler as wr
import pandas as pd
import pyarrow as pa

from constants import STDOUT, STDERR, S3_STAGING_BUCKET, S3_STAGING_PREFIX


def convert_types(df: pd.DataFrame, df_schema: dict) -> pd.DataFrame:
    """ Cast data types on the dataframe columns and overwrite some values.
    Problematic values are 'nan' and 'inf' within the numeric columns. Columns,
    which could contain 'inf', should be casted to type 'float'. For integer
    types 'nan' will be casted to 0.
    Finally, in the string columns 'None' is replaced with '' as the export
    from pandas to PARQUET would automatically cast NoneType to the 'None'
    string.
    :param df: Original dataframe to be casted.
    :param df_schema: Schema to be applied. See 'tables.py' for the simplified
        schema definition.
    :return: Casted and preprocessed dataframe.
    """
    for col in df_schema.get('int', {}).get('columns', []):
        df[col] = df[col].apply(lambda x: '0' if str(x) == 'nan' else x)

    for col in df_schema.get('bool', {}).get('columns', []):
        if col in df.columns and str(df[col].dtype) == 'object':
            df[col] = df[col].astype(dtype=str)
            mask = df[col] != 'True'
            df.loc[mask, col] = None

    for conversion in df_schema:
        tp = df_schema[conversion]['type']
        for column in df_schema[conversion]['columns']:
            try:
                df[column] = df[column].astype(dtype=tp)
                if conversion == 'str':
                    mask = df[column].isin(['None', 'nan'])
                    df.loc[mask, column] = None
            except Exception as e:
                msg = 'Failed to convert column '
                msg += f'{column} to type {conversion}!'
                print(msg)
                raise e
    return df


def export_parquet_to_s3(
        df: pd.DataFrame,
        bucket: str = S3_STAGING_BUCKET,
        prefix: str = S3_STAGING_PREFIX,
        table_name: str = None,
) -> None:
    databases = wr.catalog.databases()

    if "bce_glue" not in databases.values:
        wr.catalog.create_database("bce_glue")
        STDOUT.write(wr.catalog.databases())
    else:
        STDERR.write("Database bce_glue already exists\n")

    a = wr.catalog.tables(database="bce_glue")

    date = datetime.now().strftime("%Y/%m/%d")
    p = f"s3://{bucket}/{prefix}{table_name}/{date}"

    # writing df to Data Lake (S3 + Parquet + Glue Catalog)
    response = wr.s3.to_parquet(
        df=df,
        path=f"s3://{bucket}/{prefix}{table_name}/{date}",
        dataset=True,
        database="bce_glue",
        table=table_name,
        mode="overwrite"
    )

    if response is not None:
        STDERR.write(f"Response from to_parquet: {response}\n")



