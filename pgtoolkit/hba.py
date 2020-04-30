# coding: utf-8

""".. currentmodule:: pgtoolkit.hba

This module supports reading, validating, editing and rendering ``pg_hba.conf``
file. See `Client Authentication
<https://www.postgresql.org/docs/current/static/auth-pg-hba-conf.html>`__ in
PostgreSQL documentation for details on format and values of ``pg_hba.conf``
file.


API Reference
-------------

The main entrypoint of this API is the :func:`parse` function. It returns a
:class:`HBA` object containing :class:`HBARecord` instances.

.. autofunction:: parse
.. autoclass:: HBA
.. autoclass:: HBARecord


Examples
--------

Loading a ``pg_hba.conf`` file :

.. code:: python

    pgpass = parse('my_pg_hba.conf')

You can also pass a file-object:

.. code:: python

    with open('my_pg_hba.conf', 'r') as fo:
        hba = parse(fo)

Creating a ``pg_hba.conf`` file from scratch :

.. code:: python

    hba = HBA()
    record = HBARecord(
        conntype='local', database='all', user='all', method='peer',
    )
    hba.lines.append(record)

    with open('pg_hba.conf', 'w') as fo:
        hba.write(fo)


Using as a script
-----------------

:mod:`pgtoolkit.hba` is usable as a CLI script. It accepts a pg_hba file path
as first argument, read it, validate it and re-render it. Fields are aligned to
fit pseudo-column width. If filename is ``-``, stdin is read instead.

.. code:: console

    $ python -m pgtoolkit.hba - < data/pg_hba.conf
    # TYPE  DATABASE        USER            ADDRESS                 METHOD

    # "local" is for Unix domain socket connections only
    local   all             all                                     trust
    # IPv4 local connections:
    host    all             all             127.0.0.1/32            ident map=omicron

"""  # noqa


from __future__ import print_function

import os
import shlex
import sys
import warnings

from .errors import ParseError
from ._helpers import (
    PY2,
    open_or_return,
    open_or_stdin,
    string_types,
)


def _check_file_mode(fo):
    if not hasattr(fo, 'mode'):
        return
    if PY2 and 'b' not in fo.mode:
        raise TypeError('file should be opened in binary mode')
    elif not PY2 and 'b' in fo.mode:
        raise TypeError('file should be opened in text mode')


class HBAComment(str):
    def __repr__(self):
        return '<%s %.32s>' % (self.__class__.__name__, self)


class HBARecord(object):
    """Holds a HBA record composed of fields and a comment.

    Common fields are accessible through attribute : ``conntype``,
    ``databases``, ``users``, ``address``, ``netmask``, ``method``.
    Auth-options fields are also accessible through attribute like ``map``,
    ``ldapserver``, etc.

    ``address`` and ``netmask`` fields are not always defined. If not,
    accessing undefined attributes trigger an :exc:`AttributeError`.

    ``databases`` and ``users`` have a single value variant respectively
    :attr:`database` and :attr:`user`, computed after the list representation
    of the field.

    .. automethod:: parse
    .. automethod:: __init__
    .. automethod:: __str__
    .. automethod:: matches
    .. autoattribute:: database
    .. autoattribute:: user

    """

    COMMON_FIELDS = [
        'conntype', 'databases', 'users', 'address', 'netmask', 'method',
    ]
    CONNECTION_TYPES = ['local', 'host', 'hostssl', 'hostnossl']

    @classmethod
    def parse(cls, line):
        """Parse a HBA record

        :rtype: :class:`HBARecord` or a :class:`str` for a comment or blank
                line.
        :raises ValueError: If connection type is wrong.

        """
        line = line.strip()
        record_fields = ['conntype', 'databases', 'users']
        values = shlex.split(line, comments=False)
        # Split databases and users lists.
        values[1] = values[1].split(',')
        values[2] = values[2].split(',')
        try:
            hash_pos = values.index('#')
        except ValueError:
            comment = None
        else:
            values, comment = values[:hash_pos], values[hash_pos:]
            comment = ' '.join(comment[1:])

        if values[0] not in cls.CONNECTION_TYPES:
            raise ValueError("Unknown connection types %s" % values[0])
        if 'local' != values[0]:
            record_fields.append('address')
        common_values = [v for v in values if '=' not in v]
        if len(common_values) >= 6:
            record_fields.append('netmask')
        record_fields.append('method')
        base_options = list(zip(record_fields, values[:len(record_fields)]))
        auth_options = [o.split('=', 1) for o in values[len(record_fields):]]
        return cls(base_options + auth_options, comment=comment)

    def __init__(self, values=None, comment=None, **kw_values):
        """
        :param values: A dict of fields.
        :param kw_values: Fields passed as keyword.
        :param comment:  Comment at the end of the line.
        """
        values = dict(values or {}, **kw_values)
        if 'database' in values:
            values['databases'] = [values.pop('database')]
        if 'user' in values:
            values['users'] = [values.pop('user')]
        self.__dict__.update(values)
        self.fields = [k for k, _ in values.items()]
        self.comment = comment

    def __repr__(self):
        return '<%s %s%s>' % (
            self.__class__.__name__,
            ' '.join(self.common_values),
            '...' if self.auth_options else ''
        )

    def __str__(self):
        """Serialize a record line, without EOL."""
        # Stolen from default pg_hba.conf
        widths = [8, 16, 16, 16, 8]

        fmt = ''
        for i, field in enumerate(self.COMMON_FIELDS):
            try:
                width = widths[i]
            except IndexError:
                width = 0

            if field not in self.fields:
                fmt += ' ' * width
                continue

            if width:
                fmt += '%%(%s)-%ds ' % (field, width - 1)
            else:
                fmt += '%%(%s)s ' % (field,)
        # Serialize database and user list using property.
        values = dict(self.__dict__, databases=self.database, users=self.user)
        line = fmt.rstrip() % values

        auth_options = ['%s="%s"' % i for i in self.auth_options]
        if auth_options:
            line += ' ' + ' '.join(auth_options)

        if self.comment is not None:
            line += '  # ' + self.comment
        else:
            line = line.rstrip()

        return line

    @property
    def common_values(self):
        str_fields = self.COMMON_FIELDS[:]
        # Use serialized variant.
        str_fields[1:3] = ['database', 'user']
        return [
            getattr(self, f)
            for f in str_fields
            if f in self.fields
        ]

    @property
    def auth_options(self):
        return [
            (f, getattr(self, f))
            for f in self.fields
            if f not in self.COMMON_FIELDS
        ]

    @property
    def database(self):
        """Hold database column as a single value.

        Use `databases` attribute to get parsed database list. `database` is
        guaranteed to be a string.

        """
        return ','.join(self.databases)

    @property
    def user(self):
        """Hold user column as a single value.

        Use ``users`` property to get parsed user list. ``user`` is guaranteed
        to be a string.

        """
        return ','.join(self.users)

    def matches(self, **attrs):
        """Tells if the current record is matching provided attributes.

        :param attrs: keyword/values pairs corresponding to one or more
            HBARecord attributes (ie. user, conntype, etc…)
        """

        # Provided attributes should be comparable to HBARecord attributes
        for k in attrs.keys():
            if k not in self.COMMON_FIELDS + ['database', 'user']:
                raise AttributeError('%s is not a valid attribute' % k)

        for k, v in attrs.items():
            if getattr(self, k, None) != v:
                return False
        return True


class HBA(object):
    """Represents pg_hba.conf records

    .. attribute:: lines

        List of :class:`HBARecord` and comments.

    .. attribute:: path

        Path to a file. Is automatically set when calling :meth:`parse` with a
        path to a file. :meth:`save` will write to this file if set.

    .. automethod:: __iter__
    .. automethod:: parse
    .. automethod:: save
    .. automethod:: remove
    .. automethod:: merge
    """
    def __init__(self, entries=None):
        """HBA constructor

        :param entries: A list of HBAComment or HBARecord. Optional.
        """
        if entries and not isinstance(entries, list):
            raise ValueError('%s should be a list' % entries)
        self.lines = entries or []
        self.path = None

    def __iter__(self):
        """Iterate on records, ignoring comments and blank lines."""
        return iter(filter(lambda l: isinstance(l, HBARecord), self.lines))

    def parse(self, fo):
        """Parse records and comments from file object

        :param fo: An iterable returning lines

        The file object must be opened in "text" mode in Python 3 and "binary"
        mode in Python 2.
        """
        _check_file_mode(fo)
        for i, line in enumerate(fo):
            stripped = line.lstrip()
            if not stripped or stripped.startswith('#'):
                record = HBAComment(line.replace(os.linesep, ''))
            else:
                try:
                    record = HBARecord.parse(line)
                except Exception as e:
                    raise ParseError(1 + i, line, str(e))
            self.lines.append(record)

    def save(self, fo=None):
        """Write records and comments in a file

        :param fo: a file-like object. Is not required if :attr:`path` is set.

        Line order is preserved. Record fields are vertically aligned to match
        the columen size of column headers from default configuration file.

        The file object must be opened in "text" mode in Python 3 and "binary"
        mode in Python 2.

        .. code::

            # TYPE  DATABASE        USER            ADDRESS                 METHOD
            local   all             all                                     trust
        """  # noqa
        with open_or_return(fo or self.path, mode='w') as fo:
            _check_file_mode(fo)
            for line in self.lines:
                fo.write(str(line) + os.linesep)

    def remove(self, filter=None, **attrs):
        """Remove records matching the provided attributes.

        One can for example remove all records for which user is 'david'.

        :param filter: a function to be used as filter. It is passed the record
            to test against. If it returns True, the record is removed. It is
            kept otherwise.
        :param attrs: keyword/values pairs correspond to one or more
            HBARecord attributes (ie. user, conntype, etc...)

        Usage examples:

        .. code:: python

            hba.remove(filter=lamdba r: r.user == 'david')
            hba.remove(user='david')

        """
        if filter is not None and len(attrs.keys()):
            warnings.warn('Only filter will be taken into account')

        # Attributes list to look for must not be empty
        if filter is None and not len(attrs.keys()):
            raise ValueError('Attributes dict cannot be empty')

        filter = filter or (lambda l: l.matches(**attrs))

        self.lines = [
            l for l in self.lines
            if not (isinstance(l, HBARecord) and filter(l))
        ]

    def merge(self, other):
        """Add new records to HBAFile or replace them if they are matching
            (ie. same conntype, database, user and address)

        :param other: HBAFile to merge into the current one.
            Lines with matching conntype, database, user and database will be
            replaced by the new one. Otherwise they will be added at the end.
            Comments from the original hba are preserved.
        """
        lines = self.lines[:]
        new_lines = other.lines[:]
        other_comments = []

        for i, line in enumerate(lines):
            if isinstance(line, HBAComment):
                continue
            for new_line in new_lines:
                if isinstance(new_line, HBAComment):
                    # preserve comments until next record
                    other_comments.append(new_line)
                else:
                    kwargs = dict()
                    for a in ['conntype', 'database', 'user', 'address']:
                        if hasattr(new_line, a):
                            kwargs[a] = getattr(new_line, a)
                    if line.matches(**kwargs):
                        # replace matched line with comments + record
                        self.lines[i:i+1] = other_comments + [new_line]
                        for c in other_comments:
                            new_lines.remove(c)
                        new_lines.remove(new_line)
                        break  # found match, go to next line
                    other_comments[:] = []
        # Then add remaining new lines (not merged)
        self.lines.extend(new_lines)


def parse(file):
    """Parse a `pg_hba.conf` file.

    :param file: Either a line iterator such as a file-like object or a string
        corresponding to the path to the file to open and parse.
    :rtype: :class:`HBA`.
    """
    if isinstance(file, string_types):
        with open(file) as fo:
            hba = parse(fo)
            hba.path = file
    else:
        hba = HBA()
        hba.parse(file)
    return hba


if __name__ == '__main__':  # pragma: nocover
    argv = sys.argv[1:] + ['-']
    try:
        with open_or_stdin(argv[0]) as fo:
            hba = parse(fo)
        hba.save(sys.stdout)
    except Exception as e:
        print(str(e), file=sys.stderr)
        exit(1)
