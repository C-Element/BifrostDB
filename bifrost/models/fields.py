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


from datetime import datetime, date, time
from decimal import Decimal

from bifrost.utils import T_NONE


class FieldException(Exception):
    """
    Exception from fields error.
    :param value:
    """

    def __init__(self, value):
        Exception.__init__(self, value)


class NotSetValue(object):
    """
    For use when the value isn't set
    """
    pass


class BaseField(object):
    """
    Base class for all fields.
    :param field_name:
    :param null:
    """

    def __init__(self, field_name=None, null=False, primary_key=False,
                 default_value=NotSetValue(), choices=None, display=None):
        self._bf_field_name = field_name
        self._bf_null = null
        self._bf_default_value = default_value
        self._bf_primary_key = primary_key
        self._bf_display = display
        self._bf_value = None
        self._bf_choices = choices
        if not isinstance(default_value, NotSetValue):
            self._bf_value = self._bf_validate(default_value)

    def custom_validation(self, value):
        """
        Custom function to validade if the value can be set. Need be overrided.
        """
        return value

    def try_set(self, value):
        """
        Try set value to this field.
        :param value:
        """
        self._bf_value = self.custom_validation(self._bf_field_validate(value))

    def _bf_field_validate(self, value):
        return self._bf_validate(value)

    def _bf_validate(self, value):
        """
        Function to validade if the value can be set.
        """
        if value is None and not self._bf_null:
            if not isinstance(self._bf_default_value, NotSetValue):
                return self._bf_default_value
            raise FieldException('This field can\'t be null.')
        elif self._bf_choices and value not in self._bf_choices:
            raise FieldException('This value isn\'t in choices list.')
        return value

    def __repr__(self):
        return '<Field({})>'.format(self.__str__())

    def __str__(self):
        return '{}: {}'.format(self.__class__.__name__, self._bf_value)

    @property
    def display(self):
        """
        The format that the value will be displayed.
        """
        if self._bf_display and self._bf_value is not None:
            return self._bf_display(self.value)
        else:
            return self.value

    @property
    def field_name(self):
        """
        The name of this fild.
        """
        return self._bf_field_name

    @property
    def primary_key(self):
        """
        If this field is a  primary key.
        """
        return self._bf_primary_key

    @property
    def value(self):
        """
        The value without any formatation.
        """
        return self._bf_value


class BoolField(BaseField):
    """
    Class for boolean fields
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def custom_validation(self, value):
        value = super(BoolField, self)._bf_validate(value)
        if not (isinstance(value, bool) or isinstance(value, str)
                or isinstance(value, T_NONE)):
            raise FieldException('This field only accept boolean values.'
                                 ' Trying set ({}){}'.format(type(value),
                                                             value))
        if isinstance(value, str):
            if value not in ('Y', 'N'):
                raise FieldException('This field only accept boolean values.'
                                     ' Trying set ({}){}'.format(type(value),
                                                                 value))
            else:
                value = value == 'Y'
        return value


class BytesField(BaseField):
    """
    Class for Bytes fields
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def custom_validation(self, value):
        value = super(BytesField, self)._bf_validate(value)
        if not (isinstance(value, bytes) or isinstance(value, T_NONE)):
            raise FieldException('This field only accept bytes values.'
                                 ' Trying set ({}){}'.format(type(value),
                                                             value))
        return value


class CharField(BaseField):
    """
    Class for text fields
    """

    def __init__(self, max_length=255, accept_empty=False, **kwargs):
        super().__init__(**kwargs)
        self._bf_max_length = max_length
        self._bf_accept_empty = accept_empty

    def custom_validation(self, value):
        value = super(CharField, self)._bf_validate(value)
        if not (isinstance(value, str) or isinstance(value, T_NONE)):
            raise FieldException('This field only accept string values.'
                                 ' Trying set ({}){}'.format(type(value),
                                                             value))
        if isinstance(value, str) \
                and len(value) == 0 and not self._bf_accept_empty:
            raise FieldException('This field don\'t accept empty string.'
                                 ' Trying set ({}){}'.format(type(value),
                                                             value))
        if isinstance(value, str) \
                and len(value) > self._bf_max_length:
            raise FieldException('Value greater than the maximun allowed '
                                 '{} < {}.'.format(self._bf_max_length,
                                                   len(value)))
        return value


class DateField(BaseField):
    """
    Class for date fields
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def custom_validation(self, value):
        value = super(DateField, self)._bf_validate(value)
        if not (isinstance(value, date) or isinstance(value, tuple)
                or isinstance(value, T_NONE)):
            raise FieldException('This field only accept date values.'
                                 ' Trying set ({}){}'.format(type(value),
                                                             value))
        if isinstance(value, datetime):
            value = value.date()
        elif isinstance(value, tuple):
            if not len(value) == 2:
                raise FieldException('Wrong format tuple, '
                                     'the correct format is (str, str).'
                                     ' Trying set ({}){}'.format(type(value),
                                                                 value))
            else:
                value = datetime.strptime(value[0], value[1]).date()
        return value


class DateTimeField(BaseField):
    """
    Class for datetime fields
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def custom_validation(self, value):
        value = super(DateTimeField, self)._bf_validate(value)
        if not (isinstance(value, datetime) or isinstance(value, tuple)
                or isinstance(value, T_NONE)):
            raise FieldException('This field only accept datetime values.'
                                 ' Trying set ({}){}'.format(type(value),
                                                             value))
        elif isinstance(value, tuple):
            if not len(value) == 2:
                raise FieldException('Wrong format tuple, '
                                     'the correct format is (str, str).'
                                     ' Trying set ({}){}'.format(type(value),
                                                                 value))
            else:
                value = datetime.strptime(value[0], value[1])
        return value


class DecimalField(BaseField):
    """
    Class for decimal fields
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def custom_validation(self, value):
        value = super(DecimalField, self)._bf_validate(value)
        if not (isinstance(value, Decimal) or isinstance(value, float)
                or isinstance(value, T_NONE)):
            raise FieldException('This field only accept decimal values.'
                                 ' Trying set ({}){}'.format(type(value),
                                                             value))
        if isinstance(value, float):
            value = Decimal(str(value))
        return value


class ForeignField(BaseField):
    """
    Class for Foreign Key fields
    """

    def __init__(self, field_type, **kwargs):
        super().__init__(**kwargs)
        self._bf_field_type = field_type
        self._bf_null = False
        self._bf_value = field_type()

    def create(self):
        return self._bf_field_type()

    def custom_validation(self, value):
        value = super(ForeignField, self)._bf_validate(value)
        try:
            table = value._bf_table_name
        except AttributeError:
            tmp = self._bf_field_type()
            tmp.load(value)
            if tmp.is_new:
                raise FieldException('This field only accept models values.'
                                     ' Trying set ({}){}'.format(type(value),
                                                                 value))
            else:
                value = tmp
        return value


class IntField(BaseField):
    """
    Class for text fields
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def custom_validation(self, value):
        value = super(IntField, self)._bf_validate(value)
        if not (isinstance(value, int) or isinstance(value, T_NONE)):
            raise FieldException('This field only accept int values.'
                                 ' Trying set ({}){}'.format(type(value),
                                                             value))
        return value


class TimeField(BaseField):
    """
    Class for time fields
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def custom_validation(self, value):
        value = super(TimeField, self)._bf_validate(value)
        if not (isinstance(value, time) or isinstance(value, tuple)
                or isinstance(value, T_NONE)):
            raise FieldException('This field only accept datetime values.'
                                 ' Trying set ({}){}'.format(type(value),
                                                             value))
        elif isinstance(value, tuple):
            if not len(value) == 2:
                raise FieldException('Wrong format tuple, '
                                     'the correct format is (str, str).'
                                     ' Trying set ({}){}'.format(type(value),
                                                                 value))
            else:
                value = datetime.strptime(value[0], value[1]).time()
        return value
