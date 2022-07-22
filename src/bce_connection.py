import os
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError

from constants import STDOUT, STDERR


class BceConnection:
    server = os.environ['BCE_HOST']
    port = os.environ['BCE_PORT']
    database = os.environ['BCE_DB']
    username = os.environ['BCE_USER']
    password = os.environ['BCE_PWD']

    def bce_open_connection(self) -> sqlalchemy.engine.Engine:
        """Create a connection to Europages' BCE database.

        :return: sqlalchemy connection to BCE
        """
        conn_url = f"mysql+mysqldb://" \
                   f"{BceConnection.username}:{BceConnection.password}" \
                   f"@{BceConnection.server}:{BceConnection.port}" \
                   f"/{BceConnection.database}"

        try:
            engine = sqlalchemy.create_engine(conn_url, pool_size=20)
        except SQLAlchemyError as e:
            STDERR.write(f"Error connecting to BCE Database: {e}\n")
            sqlalchemy.session.rollback()

        STDOUT.write("Connected to BCE Database\n")

        return engine


    def bce_close_connection(self, engine):
        """Close a connection to Europages' BCE database.

        :param engine: sqlalchemy connection to BCE
        :return: closed connection
        """
        engine.dispose()
        STDERR.write(f"BCE Database connection closed.\n")
        return engine.dispose()




