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


class BaseDB:
    """
    Base class for all database connections and manipulation.
    """

    def __init__(self):
        self.connection = None
        self.address = None
        self.port = None
        self.host = None
        self.user = None
        self.db = None
        self.password = None
        self.is_valid = False

    def _verify_connection(self, host, user, password, db=None):
        """
        Do the connection with the database. Need be overrided.
        """
        pass

    def _verify_field(self, field):
        """
        Verify field value, and normalize if necessary.
        """
        return field

    def _query(self, query, params=None):
        """
        Execute a query into database and return a list of list with data.
        :param query: the query body.
        :param params: the bind variables.
        :return: :raise BaseDBException:
        """
        if not params:
            params = {}
        if not self.is_valid:
            raise BaseDBException('This isn\'t a valid connection!!!')
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        result = []
        for line in cursor.fetchall():
            data = []
            for field in line:
                data.append(self._verify_field(field))
            result.append(data)
        return result

    def _query_with_columns(self, query, params=None):
        """
        Execute a query into database and return a list as [[columns_names],
        [query_data]].
        :param query: the query body.
        :param params: the bind variables.
        :return: :raise BaseDBException:
        """
        if not params:
            params = {}
        if not self.is_valid:
            raise BaseDBException('This isn\'t a valid connection!!!')

        cursor = self.connection.cursor()
        cursor.execute(query, params)
        result = []
        columns = [name[0] for name in cursor.description]
        for line in cursor.fetchall():
            data = []
            for field in line:
                data.append(self._verify_field(field))
            result.append(data)
        return columns, result

    def _command(self, command, params=None):
        """
        Execute a non return query into database.
        :param command: the command body.
        :param params: the bind variables.
        :return: :raise BaseDBException:
        """

        if not params:
            params = {}
        if not self.is_valid:
            raise BaseDBException('This isn\'t a valid connection!!!')
        cursor = self.connection.cursor()
        cursor.execute(command, params)
        self.connection.commit()
        return True

    def close(self):
        try:
            self.connection.close()
        except:
            pass


class BaseDBException(Exception):
    """
    :param value:
    """

    def __init__(self, value):
        Exception.__init__(self, value)


class NoConnection(BaseDB):
    """
    class to set into models when dont have connection configured.
    """
    def __init__(self):
        BaseDB.__init__(self)

    def _verify_connection(self, host, user, password, db=None):
        pass

    def _verify_field(self, field):
        pass

    def query(self, query, params=None):
        raise BaseDBException('NO CONNECTION CONFIGURED!!')

    def query_with_columns(self, query, params=None):
        raise BaseDBException('NO CONNECTION CONFIGURED!!')

    def command(self, command, params=None):
        raise BaseDBException('NO CONNECTION CONFIGURED!!')
