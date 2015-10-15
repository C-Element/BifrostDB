import pymssql

from bifrost.db.basedb import BaseDB, BaseDBException


class MSs(BaseDB):
    """
Class for SQL Server database.
    :param host: database host address.
    :param user: database username.
    :param password: database password.
    :param db: database name.
    """

    def __init__(self, host, user, password, db):
        BaseDB.__init__(self)
        self._verify_connection(host, user, password, db)

    def _verify_connection(self, host, user, password, db=None):
        """
        Do the connection with the database.
        """
        try:
            self.connection = pymssql.connect(user=user, password=password,
                                              server=host, database=db)
            self.host = host
            self.user = user
            self.password = password
            self.db = db
            self.is_valid = True
        except pymssql.DatabaseError:
            self.is_valid = False
        except Exception as ee:
            print(ee)
            self.is_valid = False

    def query(self, query, params=None):
        """
Execute a query into database.
    :param query: query string to be executed.
    :param params: binding variables.
    :return: array of tuples with query data.
        """
        try:
            return self._query(query, params)
        except pymssql.DatabaseError as e:
            print(e)
            raise e
        except Exception as ee:
            print(ee)
            raise ee

    def query_with_columns(self, query, params=None):
        """
Execute a query into database.
    :param query: query string to be executed.
    :param params: binding variables.
    :return: array of tuples with query data.
        """
        try:
            return self._query_with_columns(query, params)
        except pymssql.DatabaseError as e:
            print(e)
            raise e
        except Exception as ee:
            print(ee)
            raise ee

    def command(self, command, params=None):
        """
Execute a command into database.
    :param command: command string to be executed.
    :param params: binding variables.
        """
        try:
            return self._command(command, params)
        except pymssql.IntegrityError as e:
            se = str(e)
            if str(e).startswith('duplicate key value '
                                 'violates unique constraint'):
                raise BaseDBException('DUPLICATE KEY\n' + se)

        except pymssql.DatabaseError as e:
            print(e)
            raise e
        except Exception as ee:
            print(ee)
            raise ee
