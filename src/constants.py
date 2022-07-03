import os
import sys

STDOUT = sys.stdout
STDERR = sys.stderr

S3_BUCKET = os.getenv('S3_BUCKET')
CHUNKSIZE = 5000000
REGEX_PATERN = '[0-9]|\[ns\]|\[day\]'

# hard coded for testing purposes only
table_name = "post"