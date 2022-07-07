import os
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError

from constants import STDOUT, STDERR


def bce_connection(
        server: str = os.environ['BCE_HOST'],
        port: str = os.environ['BCE_PORT'],
        database: str = os.environ['BCE_DB'],
        username: str = os.environ['BCE_USER'],
        password: str = os.environ['BCE_PWD'],
) -> sqlalchemy.engine.Engine:
    """ Create a connection to Europages' BCE database.
    :param server: URL/address of the database.
    :param port: Port of the database.
    :param database: Name of the database.
    :param username: User with read permission.
    :param password: User's respective password.
    :return: Connection to be used by Pandas.
    """
    conn_url = f"mysql+mysqldb://" \
               f"{username}:{password}@{server}:{port}/{database}"

    try:
        engine = sqlalchemy.create_engine(conn_url, pool_size=20)
    except SQLAlchemyError as e:
        STDERR.write(f"Error connecting to BCE Database: {e}\n")
        sqlalchemy.session.rollback()

    STDOUT.write("Connected to BCE Database\n")

    return engine



