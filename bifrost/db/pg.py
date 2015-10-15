# Copyright (C) 2015 Clemente Junior
#
# This file is part of BifrostDB
#
# BifrostDB is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Foobar is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Foobar.  If not, see <http://www.gnu.org/licenses/>.

import psycopg2

from bifrost.db.basedb import BaseDB, BaseDBException


class PgDB(BaseDB):
    """
Class for PostgreSQL database.
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
            self.connection = psycopg2.connect("dbname=%(db)s user=%(user)s "
                                               "password=%(password)s "
                                               "host=%(host)s" %
                                               {'db': db, 'user': user,
                                                'password': password,
                                                'host': host})
            self.host = host
            self.user = user
            self.password = password
            self.db = db
            self.is_valid = True
        except psycopg2.DatabaseError:
            self.is_valid = False
        except Exception as ee:
            print(ee)
            self.is_valid = False

    def _verify_field(self, field):
        """
        Verify field value, and normalize if necessary.
        """
        if isinstance(field, memoryview):
            return field.tobytes()
        return field

    def query(self, query, params=None):
        """
Execute a query into database.
    :param query: query string to be executed.
    :param params: binding variables.
    :return: array of tuples with query data.
        """
        try:
            return self._query(query, params)
        except psycopg2.DatabaseError as e:
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
    :return: array where the first slot have the columns names of the query and
             on second an array of tuples with query data.
        """
        try:
            return self._query_with_columns(query, params)
        except psycopg2.DatabaseError as e:
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
        except psycopg2.IntegrityError as e:
            se = str(e)
            if str(e).startswith('duplicate key value '
                                 'violates unique constraint'):
                raise BaseDBException('DUPLICATE KEY\n' + se)
            else:
                raise psycopg2.IntegrityError(se)
        except psycopg2.DatabaseError as e:
            print(e)
            raise e
        except Exception as ee:
            print(ee)
            raise ee
