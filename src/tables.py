from typing import Dict

import pyarrow as pa


post_schema = {
    'str': {
        'columns': [
            'POST_SPEC_EVENT_START_DATE', 'POST_SPEC_EVENT_END_DATE', 'POST_TITLE',
            'POST_TEXT', 'POST_LINK', 'POST_REF_TYPE_CUSTOM',
            'POST_IMAGE_FILESTORE',
        ],
        'type': str,
    },
    'int': {
        'columns': [
            'POST_ID', 'POST_REF_TYPE', 'POST_REF_LANGUAGE',
            'POST_ACTIVATED',
        ],
        'type': int,
    },
    'date': {
        'columns': ['POST_CREATE_DATE'],
        'type': 'datetime64[D]'
    },
}

publication_schema = {
    'str': {
        'columns': [
            'tpu_id', 'S_ID', 'IDTIERS', 'ICODE', 'TCODE',
            'PUB_S_ID_TIERS', 'PUB_U_ID_TIERS',
        ],
        'type': str,
    },
    'int': {
        'columns': [
            'PUB_ID', 'CMP_ID', 'OFF_ID', 'SPU_ID', 'PUB_WEB_PRIORITE',
        ],
        'type': int,
    },
    'float': {
        'columns': [
            'EDITION', 'CTR_ID',
        ],
        'type': float,
    },
    'date': {
        'columns': [
            'PUB_DT_CREATION', 'PUB_DT_DEBUT', 'PUB_DT_FIN', 'PUB_DT_COMPLETE',
            'PUB_DT_DEBUT_VERSION', 'PUB_DT_FIN_VERSION', 'PUB_DT_ENVOI_WEB',
            'PUB_DT_RETOUR_WEB',
        ],
        'type': 'datetime64[D]'
    },
}


def get_pyarrow_schema(schema: Dict[str, object]) -> pa.Schema:
    """
    :param schema:
    :return:
    """
    types = {
        'str': pa.string(),
        'int': pa.int64(),
        'float': pa.float64(),
        'bool': pa.bool_(),
        'date': pa.date32(),
        'timestamp': pa.timestamp('s', tz='Europe/Berlin')
    }

    pyarrow_schema = list()
    for tp in schema:
        [pyarrow_schema.append((col, types[tp])) for col in schema[tp]['columns']]

    return pa.schema(pyarrow_schema)


query_schema = {
    "post": (
        post_schema,
        get_pyarrow_schema(post_schema)
    ),
    "publication": (
        publication_schema,
        get_pyarrow_schema(publication_schema)
    ),
}


