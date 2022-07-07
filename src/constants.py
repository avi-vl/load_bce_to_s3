import sys

STDOUT = sys.stdout
STDERR = sys.stderr

CHUNKSIZE = 5000000
REGEX_PATERN = '[0-9]|\[ns\]|\[day\]'

S3_STAGING_BUCKET = "bi-visable-input"
S3_STAGING_PREFIX = "bce/"

# hard coded for testing purposes only
table_name = "publication"
