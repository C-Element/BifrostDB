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

T_NONE = type(None)


def normalize_datetime(value, change_to=''):
    """
Return a string representation of an date/datetime object.
    :param value:
    :param change_to:
    :return:
    """
    if isinstance(value, datetime):
        return value.strftime("%d/%m/%Y %H:%M")
    elif isinstance(value, date):
        return value.strftime("%d/%m/%Y")
    elif isinstance(value, time):
        return value.strftime("%H:%M")
    return replace_when_none(value, change_to=change_to)


def normalize_db_values(data, cls=None):
    """
Normalize data values to work according to Bifrost.
    :param data: dictionary to be iterated.
    :return: a dictionary with its values normalized.
    """
    new_dict = {}
    sep = '__'
    for key in data.keys():
        current = data[key]
        if cls:
            if sep in key:
                part1, part2 = key.split(sep)
                key = cls._bf_objects_fields[part1] + sep + part2
            else:
                key = cls._bf_objects_fields[key]
        if isinstance(current, bool):
            new_dict[key] = 'Y' if current else 'N'
        else:
            try:
                tmp = current._get_primary_key()
                current = tmp[1]
            except:
                pass
            new_dict[key] = current
    return new_dict


def not_empty(text):
    """
Verify if text isn't empty
    :param text: text to be evalueted.
    :return:True if: the string isn't empty or all this characters are space.
    """
    if text is None:
        return False
    elif not isinstance(text, str):
        text = str(text)
    return False if text == '' or len(text.replace(' ', '')) == 0 else True


def nwe(value):
    """
Return None if value is an empty string.
    :param value: string to be evalueted.
    :return: None if value is an empty string.
    """
    return value if not_empty(value) else None


def replace_when_none(obja, objb):
    """
Return objb if obja is None.
    :param obja: object to be evalueted.
    :param objb: object to be replaced.
    :return: objb if obja is None, else return obja.
    """
    return objb if obja is None else obja


# Class
class ObjectNotSavedException(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)


class ReadOnlyException(Exception):
    def __init__(self, message='Can\'t assign a read-only attribute'):
        Exception.__init__(self, message)


class WrongTypeException(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
