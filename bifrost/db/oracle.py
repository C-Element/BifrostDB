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

import cx_Oracle

from bifrost.db.basedb import BaseDB
from bifrost.utils import replace_when_none


class OracleDB(BaseDB):
    def __init__(self, host, user, password):
        BaseDB.__init__(self)
        self._verify_connection(host, user, password)

    def _verify_connection(self, host, user, password, db=None):
        """
        Do the connection with the database.
        """
        try:
            self.connection = cx_Oracle.connect(user, password, host)
            self.host = host
            self.user = user
            self.password = password
            self.is_valid = True
        except cx_Oracle.DatabaseError:
            self.is_valid = False
        except Exception as ee:
            print(ee)
            self.is_valid = False

    def _verify_field(self, field):
        """
        Verify field value, and normalize if necessary.
        """
        if isinstance(field, cx_Oracle.LOB):
            return field.read()
        return field

    def query(self, query, params=None, encoding='cp1252'):
        """
Execute a query into database.
    :param query: query string to be executed.
    :param params: binding variables.
    :return: array of tuples with query data.
        """
        try:
            new_params = {}
            for p in replace_when_none(params, {}):
                if not p.startswith(':'):
                    new_params[':{}'.format(p)] = params[p]
                else:
                    new_params[p] = params[p]
            return self._query(query.encode(encoding), new_params)
        except cx_Oracle.DatabaseError as e:
            print(e)
            raise e
        except Exception as ee:
            print(ee)
            raise ee

    def query_with_columns(self, query, params=None, encoding='cp1252'):
        """
Execute a query into database.
    :param query: query string to be executed.
    :param params: binding variables.
    :return: array of tuples with query data.
        """
        try:
            new_params = {}
            for p in replace_when_none(params, {}):
                if not p.startswith(':'):
                    new_params[':{}'.format(p)] = params[p]
                else:
                    new_params[p] = params[p]

            return self._query_with_columns(query.encode(encoding), new_params)
        except cx_Oracle.DatabaseError as e:
            print(e)
            raise e
        except Exception as ee:
            print(ee)
            raise ee

    def command(self, command, params=None, encoding='cp1252'):
        """
Execute a command into database.
    :param command: command string to be executed.
    :param params: binding variables.
        """
        try:
            new_params = {}
            for p in replace_when_none(params, {}):
                if not p.startswith(':'):
                    new_params[':{}'.format(p)] = params[p]
                else:
                    new_params[p] = params[p]
            return self._command(command.encode(encoding), new_params)
        except cx_Oracle.DatabaseError as e:
            print(e)
            raise e
        except Exception as ee:
            print(ee)
            raise ee
