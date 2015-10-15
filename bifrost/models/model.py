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

from bifrost.db.basedb import BaseDBException, NoConnection
from bifrost.db.query import Query
from bifrost.models.fields import BaseField, ForeignField, FieldException
from bifrost.utils import ObjectNotSavedException, replace_when_none, \
    normalize_db_values


class BaseModel(object):
    """ Base class for all objects that involves databases. """

    def __init__(self):
        self._bf_fields_objects = {}
        self._bf_is_new = True
        self._bf_objects_fields = {}
        self._bf_old_data = {}
        self._bf_primary_key_name = ''
        self._bf_table_name = self.__class__.__name__
        self.create_connection = NoConnection

    def bf_get_all_fields(self):
        """
        Return a dict with all Bifrost fields from the class.
        """
        to_return = {}
        for key in self.__dict__:
            tmp = super(BaseModel, self).__getattribute__(key)
            if isinstance(tmp, BaseField):
                to_return[key] = tmp
        return to_return

    def bf_prepare(self):
        """
        Prepare the object as a Bifrost model's
        :return: None
        """
        fields = self.bf_get_all_fields()
        for field in fields:
            if fields[field]._bf_primary_key:
                self._bf_primary_key_name = field
            self._bf_objects_fields[field] = replace_when_none(
                fields[field].field_name, field)
            self._bf_fields_objects[
                replace_when_none(fields[field].field_name, field)] = field

    def load(self, pk):
        """
        Load a object where field_primary_key = pk.
        :param pk: the primary key value.
        """

        primary_key = self._get_primary_key()
        if primary_key:
            connection = self.create_connection()
            result = connection.query_with_columns(
                '{0} "{1}" = %({1})s'.format(self.qry_init_part,
                                             primary_key[0]),
                {primary_key[0]: pk})
            if len(result) > 0:
                self.load_data(dict(zip(result[0], result[1][0])))
            connection.close()
            self.on_load()

    def load_data(self, data):

        """
        Fill the object with your data .
        :param data: dictionary as {column_name: value, ...}
        """
        self._bf_old_data.clear()
        for key in data:
            try:
                obj_name = self._bf_fields_objects[key]
                tmp = super(BaseModel, self).__getattribute__(obj_name)
                if isinstance(tmp, ForeignField):
                    cls = tmp.create()
                    query = Query(cls)
                    query.get(**{cls._bf_primary_key_name: data[key]})
                    tmp.try_set(query[0])
                else:
                    tmp.try_set(data[key])
                self._bf_old_data['__bf_old__' + key] = data[key]
            except FieldException as ex:
                raise FieldException('Field {}: {}'.format(key, ex))
        self._bf_is_new = False
        self.on_load()

    def on_load(self):
        """
        Execute that functions when the object is loaded.
        """
        pass

    def on_save(self):
        """
        Execute that functions when the object is saved.
        """
        pass

    def normalize_column(self, column_name):
        """
        Normalize column according with your database.
        :param column_name: the column name.
        :return: a string with normalized column name.
        """
        if column_name.startswith('('):
            exp = column_name.split(' ')
            name = exp.pop(-1)
            return ' '.join(exp) + ' "{}"'.format(name)
        return '"{}"'.format(column_name)

    def normalize_columns(self, columns_name):
        """
        Normalize an array of columns depending with your database.
        :param columns_name: a list-like containing the columns names.
        :return: a string with normalizeds columns names.
        """
        to_return = ''
        for column_name in columns_name:
            if column_name.startswith('('):
                exp = column_name.split(' ')
                name = exp.pop(-1)
                to_return += ' '.join(exp) + ' "{}", '.format(name)
            else:
                to_return += '"{}", '.format(column_name)
        return to_return.rstrip(', ')

    def save(self):
        """
        Save data to the database.
        :raise ObjectNotSavedException:
        """
        connection = self.create_connection()
        command = self._save_string()
        data = normalize_db_values(self._data_dict(), self)
        data.update(normalize_db_values(self._bf_old_data))
        try:
            connection.command(command, data)
        except BaseDBException as err:
            if str(err).startswith('DUPLICATE KEY'):
                raise ObjectNotSavedException('Duplicated Item.')
        connection.close()
        if self._bf_is_new:
            self._load_id_by_all_fields()
        self._bf_is_new = False
        self.on_save()

    def _data_dict(self, with_primary_key=False):
        """ Return adictionary with bind variables and yours values. """
        fields = self.bf_get_all_fields()
        data = {}
        if self._bf_primary_key_name and not with_primary_key:
            fields.pop(self._bf_primary_key_name)
        for key in fields:
            value = fields[key].value
            if isinstance(value, BaseModel):
                value = value.__getattribute__(value._bf_primary_key_name)
            data[key] = value
        return data

    def _get_primary_key(self):
        """
        Get primary key data.
        :return: a list as [primary_key_db_name, primary_key_value]
        """
        if self._bf_primary_key_name:
            tmp = super(BaseModel,
                        self).__getattribute__(self._bf_primary_key_name)
            return replace_when_none(tmp.field_name,
                                     self._bf_primary_key_name), tmp.value
        return None

    def _insert_string(self):

        """ Return the insert string. """
        keys = normalize_db_values(self._data_dict(), self).keys()
        return 'INSERT INTO {} ({}) VALUES ({})'.format(
            self._bf_table_name, '"{}"'.format('", "'.join(keys)),
            '%({})s'.format(')s, %('.join(keys)))

    def _load_id_by_all_fields(self):

        """
        Load a id from this object values.
         """
        if self._bf_primary_key_name:
            qry = Query(self)

            qry.get(**self._data_dict())
            if len(qry) > 0:
                tmp = qry[0]
                super(BaseModel, self).__setattr__(
                    self._bf_primary_key_name,
                    tmp.__dict__[self._bf_primary_key_name])

    def _save_string(self):

        """ Return the save [insert or update] string. """

        if self._bf_is_new:
            return self._insert_string()
        else:
            return self._update_string()

    def _update_string(self):
        """ Return the update string. """
        keys = normalize_db_values(self._data_dict(), self).keys()
        to_set = ''
        where = ''
        for key in keys:
            to_set += ' {}=%({})s,'.format(self.normalize_column(key), key)
        for key in self._bf_old_data:
            if self._bf_old_data[key] is None:
                where += ' {} is %({})s AND'.format(self.normalize_column(
                    key.replace('__bf_old__', '', 1)), key)
            else:
                where += ' {} = %({})s AND'.format(self.normalize_column(
                    key.replace('__bf_old__', '', 1)), key)
        cmd = 'UPDATE {} SET {} WHERE {}'.format(self._bf_table_name,
                                                 to_set.strip(','),
                                                 where.strip('AND'))
        return cmd

    @property
    def db_fields(self):
        """
        return all bifrost fields names from this object.
        """
        return self._bf_fields_objects.keys()

    @property
    def fields(self):
        """
        return all database fields names from this object.
        """
        return self._bf_objects_fields.keys()

    @property
    def is_new(self):
        """
        Verify if this object is new.
        """
        return self._bf_is_new

    @property
    def qry_init_part(self):
        """ Build the query string for select. """
        keys = normalize_db_values(self._data_dict(with_primary_key=True),
                                   self).keys()
        text = 'SELECT {} FROM {} WHERE '.format(
            '"{}"'.format('", "'.join(keys)), self._bf_table_name)
        return text

    @property
    def table_name(self):
        """
        Return the database table name from this model.
        """
        return self._bf_table_name

    def __getattribute__(self, item):
        tmp = super(BaseModel, self).__getattribute__(item)
        if isinstance(tmp, BaseField):
            return tmp.display
        else:
            return tmp

    def __repr__(self):
        return '<Model({})>'.format(self.__str__())

    def __setattr__(self, key, value):
        if key in self.__dict__ and \
                isinstance(self.__dict__[key], BaseField):
            tmp = super(BaseModel, self).__getattribute__(key)
            tmp.try_set(value)
        else:
            super(BaseModel, self).__setattr__(key, value)

    def __str__(self):
        text = ''
        data = self._data_dict(with_primary_key=True)
        count = 0
        max_items = 3
        if self._bf_primary_key_name in data:
            text += ' {}={}, '.format(self._bf_primary_key_name,
                                      data[self._bf_primary_key_name])
            count = 1
        for key in sorted(data.keys(), key=lambda x: x):
            if count == max_items or count == len(data):
                break
            elif key != self._bf_primary_key_name:
                text += ' {}={},'.format(key, data[key])
            count += 1
        return '{}:{}{}'.format(self.__class__.__name__, text.strip(','),
                                ', ...' if len(data) > max_items else '')


class OracleModel(BaseModel):
    """ Base class for all objects that involves Oracle databases. """

    def __init__(self):
        BaseModel.__init__(self)

    def load(self, pk):
        """
        Load a object where field_primary_key = pk.
        :param pk: the primary key value.
        """
        primary_key = self._get_primary_key()
        if primary_key:
            connection = self.create_connection()
            result = connection.query(
                '{0} {1} = :{1}'.format(self.qry_init_part,
                                        primary_key[0]),
                {primary_key[0]: primary_key[1].value})
            if len(result) > 0:
                self.load_data(result[0])
            connection.close()
            self.on_load()

    def normalize_column(self, column_name):
        """
        Normalize column according with your database.
        :param column_name: the column name.
        :return: a string with normalized column name.
        """
        return self._bf_objects_fields[column_name]

    def normalize_columns(self, columns_name):
        """
        Normalize an array of columns depending with your database.
        :param columns_name: a list-like containing the columns names.
        :return: a string with normalizeds columns names.
        """
        to_return = ''
        for column_name in columns_name:
            if column_name.startswith('('):
                exp = column_name.split(' ')
                name = exp.pop(-1)
                to_return += ' '.join(exp) + ' "{}", '.format(name)
            else:
                to_return += '{}, '.format(
                    self._bf_objects_fields[column_name])
        return to_return.rstrip(', ')

    def _insert_string(self):
        """ Return the insert string. """
        keys = self._data_dict().keys()
        return 'INSERT INTO {} ({}) VALUES ({})'.format(
            self._bf_table_name, '"{}"'.format('", "'.join(keys)),
            ':{}'.format(', :'.join(keys)))

    def _update_string(self):
        """ Return the update string. """
        keys = self._data_dict().keys()
        cmd = 'UPDATE {} SET'.format(self._bf_table_name)
        for key in keys:
            cmd += ' "{0}"=:{0},'.format(key)
        cmd = '{} WHERE '.format(cmd.strip(','))
        return cmd

    @property
    def qry_init_part(self):
        """ Build the query string for select. """
        keys = self._data_dict().keys()
        """ Build the query string for select. """
        text = 'SELECT {} FROM {} WHERE '.format(keys, self._bf_table_name)
        return text
