from datetime import datetime

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
        pyarrow_schema: pa.Schema,
        bucket: str = S3_STAGING_BUCKET,
        prefix: str = S3_STAGING_PREFIX,
        table_name: str = None,
) -> None:

    date = datetime.now().strftime("%Y/%m/%d")
    path = f"s3://{bucket}/{prefix}{table_name}/{date}"
    STDOUT.write(f'Exporting to path "{path}"\n')

    response = df.to_parquet(
        path=path,
        engine="pyarrow",
        schema=pyarrow_schema,
        use_deprecated_int96_timestamps=True,
        allow_truncated_timestamps=True,
        coerce_timestamps="ms",
    )

    if response is not None:
        STDERR.write(response + "\n")


