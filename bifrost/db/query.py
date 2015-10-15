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

from bifrost.utils import replace_when_none, nwe, normalize_db_values

T_CLASS = 'class'
T_LIST = 'list'
T_DICT = 'dict'


class Query(object):
    """
    Query Object

    """

    def __init__(self, obj):
        if type(obj) == type:
            obj = obj()
        self._obj = obj
        self._qry_init_part = obj.qry_init_part
        self._resultset = ()
        self._order_by = ''
        self._custom_qry_init_part = ''
        self._where_opt = {'not': ' <> ', 'like': ' like ',
                           'not_like': 'not like ', 'lt': '<', 'lte': '<=',
                           'gt': '>', 'gte': '>=', 'in': 'in',
                           'not_in': 'not in'}

    def delete_all(self):
        """
    Delete all rows of objects stored in this query. Only if the query are made
    in T_CLASS format, otherwise will rise an AttributeError exception.
    :return:
        """
        count = 0
        connection = self._obj.create_connection()
        for obj in self._resultset:
            count_attr = 0
            data = normalize_db_values(obj._data_dict(), self._obj)
            cmd = "DELETE FROM {} WHERE".format(self._obj.table_name)
            for key in data:
                if data[key] is None:
                    cmd += ' {} is %({})s AND'.format(
                        obj.normalize_column(key), key)
                else:
                    cmd += ' {}=%({})s AND'.format(obj.normalize_column(key),
                                                   key)
                count_attr += 1
            if cmd.endswith(' AND'):
                cmd = cmd[0:len(cmd) - 4]
            connection.command(cmd, data)
            count += 1
        connection.close()
        self._resultset = ()
        return count

    def get(self, result_type=T_CLASS, **where_clauses):
        """
    Get objects from specifieds clauses.
    :param result_type: type to store in resultset:
                        'class' for a model.
                        'dict' for a dictionary
                        'list' for a list.
    :param where_clauses: clauses according with model fields.
    :return:
        """
        connection = self._obj.create_connection()
        query = replace_when_none(nwe(self._custom_qry_init_part), self._qry_init_part)
        where_clauses = normalize_db_values(where_clauses, self._obj)
        self._resultset = ()
        result_type = T_DICT if self._custom_qry_init_part else result_type
        if len(where_clauses) == 0:
            query += ' 1=1'
        else:
            for key in where_clauses.keys():
                sep = '__'
                if sep in key:
                    opt = key.split(sep)[-1]
                    if opt in self._where_opt:
                        if where_clauses[key] is None and opt == 'not':
                            query += ' {} is not %({})s ' \
                                     'AND'.format(self._obj.normalize_column(
                                key.split(sep + opt)[0]), key)
                        else:
                            query += ' {} {} %({})s AND'.format(
                                self._obj.normalize_column(
                                    key.split(sep + opt)[0]),
                                self._where_opt[opt], key)
                        continue
                if where_clauses[key] is None:
                    query += ' {} is %({})s AND'.format(
                        self._obj.normalize_column(key), key)
                else:
                    query += ' {} = %({})s AND'.format(
                        self._obj.normalize_column(key), key)
        query = query.strip('AND') + self._order_by
        if result_type == T_CLASS:
            self._populate_dict(connection.query_with_columns(query,
                                                              where_clauses))
        elif result_type == T_DICT:
            self._populate_dict(
                connection.query_with_columns(query, where_clauses),
                result_type)
        else:
            self._populate(connection.query(query, where_clauses))
        connection.close()
        self._order_by = ''
        self._custom_qry_init_part = ''
        return self

    def only(self, **restrictions):
        """
        Work like get() into the resultset after the query was filled.
        :param restrictions: restriction according with model fields.
        :return:
        """
        to_return = []
        for row in self._resultset:
            can_entry = True
            for key in restrictions:
                if row.__getattribute__(key) != restrictions[key]:
                    can_entry = False
            if can_entry:
                to_return.append(row)
        return to_return

    def order(self, *args):
        """
        Insert a order by into the query.
        :param args: ordering clauses.
        :return:
        """
        if len(args) > 0:
            new_order = ' order by'
            for column in args:
                asc_desc = 'asc'
                if column.startswith('-'):
                    asc_desc = 'desc'
                    column = column[1:]
                new_order += ' {} {},'.format(
                    self._obj.normalize_column(column), asc_desc)
            self._order_by = new_order.strip(',')
        return self

    def select(self, select_options, distinct=False):
        """
    Select only specified filds
    :param select_options:
    :param distinct:
    :return:
        """
        self._custom_qry_init_part = 'SELECT {}{} FROM {} WHERE '.format(
            'DISTINCT ' if distinct else '',
            self._obj.normalize_columns(select_options), self._obj.table_name)
        return self

    def _populate(self, data):
        """
        Populate the resultset with data.
        :param data: query data.
        :return:
        """
        new_reultset = []
        for row in data:
            new_reultset.append(row)
        self._resultset = tuple(new_reultset)

    def _populate_dict(self, data, result_type=T_CLASS):
        """
        Populate the resultset with data.
        :param data: query with columns data.
        :return:
        """

        new_reultset = []
        columns = data[0]
        rset = data[1]
        for row in rset:
            if result_type == T_CLASS:
                obj = self._obj.__class__()
                obj.load_data(dict(zip(columns, row)))
                new_reultset.append(obj)
            else:
                new_reultset.append(dict(zip(columns, row)))
        self._resultset = tuple(new_reultset)

    def __getitem__(self, item):
        return self._resultset[item]

    def __iter__(self):
        return self._resultset.__iter__()

    def __len__(self):
        return len(self._resultset)

    def __repr__(self):
        return '<{}>'.format(self.__str__())

    def __str__(self):
        return 'Query: {}'.format(str(self._resultset))
