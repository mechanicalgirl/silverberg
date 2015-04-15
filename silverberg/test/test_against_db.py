# -*- coding: utf-8 -*-

# Copyright 2012 Rackspace Hosting, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Tests against an actual DB, if there is one installed and running.

Checks environment variable CASSANDRA_ENDPOINT to see how to connect to
cassandra.  If not provided, these tests do not run.
"""

from __future__ import print_function

import cql
import os
import time

from datetime import datetime
from uuid import uuid1

from twisted.internet import reactor
from twisted.internet.endpoints import clientFromString
from twisted.trial.unittest import TestCase

from silverberg.client import ConsistencyLevel, TestingCQLClient


_now = datetime.utcnow()
# truncate to milliseconds, because cassandra can only handle millsecond
# granularity
_now_expected = datetime(_now.year, _now.month, _now.day, _now.hour,
                         _now.minute, _now.second,
                         _now.microsecond / 1000 * 1000)
_uuid1 = uuid1()


cql3_types = {
    'ascii': {
        'description': 'US-ASCII character string',
        'insert': 'abcdefg',
        'expected': 'abcdefg'
    },
    'bigint': {
        'description': '64-bit signed long',
        'insert': 9223372036854775805,
        'expected': 9223372036854775805,
    },
    'blob': {
        'description': ('Arbitrary bytes (no validation), '
                        'expressed as hexadecimal'),
        'insert': '6d796b657931',
        'expected': 'mykey1'
    },
    'boolean': {
        'description': 'true or false',
        'insert': True,
        'expected': True
    },
    'counter': {
        'description': 'Distributed counter value (64-bit long)',
        'insert': 1,
        'expected': 1
    },
    'decimal': {
        'description': 'Variable-precision decimal',
        'insert': 1.513623,
        'expected': 1.513623,
        'supported': False
    },
    'double': {
        'description': '64-bit IEEE-754 floating point',
        'insert': 1.513623614,
        'expected': 1.513623614,
    },
    'float': {
        'description': '32-bit IEEE-754 floating point',
        'insert': 136.36460918,
        'expected': 136.36460918,
    },
    'inet': {
        'description': 'IP address string in IPv4 or IPv6 format*',
        'insert': '152.12.14.1',
        'expected': '152.12.14.1',
        'supported': False
    },
    'int': {
        'description': '32-bit signed integer',
        'insert': 5,
        'expected': 5
    },
    'list<int>': {
        'description': 'collection of one or more ordered elements',
        'insert': [1, 1, 2, 2, 3, 3, 4, 4],
        'expected': [1, 1, 2, 2, 3, 3, 4, 4]
    },
    'map<int, text>': {
        'description': ('A JSON-style array of literals: '
                        '{ literal : literal, ... }'),
        'insert': {1: 'whats', 2: 'up'},
        'expected': {1: 'whats', 2: 'up'}
    },
    'set<int>': {
        'description': 'A collection of one or more elements',
        'insert': set([1, 2, 3, 4]),
        'expected': set([1, 2, 3, 4])
    },
    'text': {
        'description': 'UTF-8 encoded string',
        'insert': u'(づ｡◕‿‿◕｡)づ',
        'expected': u'(づ｡◕‿‿◕｡)づ'
    },
    'timestamp': {
        'description': 'Date plus time, encoded as 8 bytes since epoch',
        'insert': _now,
        'expected': _now_expected
    },
    'uuid': {
        'description': 'A UUID in standard UUID format',
        'insert': _uuid1,
        'expected': _uuid1
    },
    'timeuuid': {
        'description': 'Type 1 UUID only (CQL 3)',
        'insert': _uuid1,
        'expected': _uuid1
    },
    'varchar': {
        'description': 'UTF-8 encoded string',
        'insert': u'(づ｡◕‿‿◕｡)づ',
        'expected': u'(づ｡◕‿‿◕｡)づ'
    },
    'varint': {
        'description': 'Arbitrary-precision integer',
        'insert': 15984362469,
        'expected': 15984362469
    },
}


def short_name(cql_type):
    """
    Return the short name given a key type (ignores the type declaration
    of collection types)
    """
    return cql_type.split('<', 1)[0]


def set_up_cassandra(endpoint, ks="test_marshalling"):
    """
    Create keyspace, a table with one column of each time, insert some
    values into all of them, and test unmarshalling
    """
    host, port = endpoint.split(':')
    connection = cql.connect(host, port, cql_version="3.0.0")
    cursor = connection.cursor()
    try:
        cursor.execute("DROP KEYSPACE {0};".format(ks))
    except:
        pass

    cursor.execute(
        "CREATE KEYSPACE {0} WITH replication = ".format(ks) +
        "{'class':'SimpleStrategy', 'replication_factor':1}")

    cursor.execute("USE {0};".format(ks))

    cursor.execute(
        "CREATE TABLE test ({0});".format(', '.join(
            ["test_key timestamp PRIMARY KEY"] +
            ['{1}_type {0}'.format(k, short_name(k))
             for k in cql3_types.keys() if k != "counter"]
        )))

    # counters are special and cannot be used in column families that contain
    # any other non-primary-key values.  Counters also can't be primary keys
    cursor.execute(
        "CREATE TABLE test_counter ("
        "test_key int PRIMARY KEY, counter_type counter);")

    cursor.close()

    return TestingCQLClient(
        clientFromString(reactor, 'tcp:{0}:{1}'.format(host, port)), ks)


_endpoint = os.getenv('CASSANDRA_ENDPOINT')
client = None
if _endpoint:
    client = set_up_cassandra(_endpoint)


def execute_insert(table_name, column_name, value, primary_key):
    """
    Execute an insert value into a column in a particular table, using
    a unique primary key.
    """
    params = {"key": primary_key, "val": value}
    if column_name == "counter_type":  # counters can only be set
        query = ("UPDATE {0} SET {1} = {1} + :val WHERE test_key = :key;"
                 .format(table_name, column_name))

    # not sure why sets are not marshalled right, but putting a set in
    # results in an error
    elif column_name == "set_type":
        query = ("INSERT INTO {0} (test_key, {1}) values (:key, {2});"
                 .format(table_name, column_name,
                         '{' + ", ".join([str(x) for x in value]) + '}'))
        params.pop("val")
    else:
        query = ("INSERT INTO {0} (test_key, {1}) values (:key, :val);"
                 .format(table_name, column_name))

    return client.execute(query, params, consistency=ConsistencyLevel.ALL)


def execute_select(table_name, column_name, primary_key):
    """
    Execute a select from a column in a particular table, using
    a particular primary key value.
    """
    return client.execute(
        "SELECT {0} FROM {1} WHERE test_key=:key;"
        .format(column_name, table_name),
        {"key": primary_key},
        consistency=ConsistencyLevel.ALL)


class UnmarshallingTestCase(TestCase):
    """
    Actually tests unmarshalling against a real Cassandra database - test
    cases are dynamically added down below.
    """
    def setUp(self):
        """
        Set up a test key ID, and also resume the client.  Set a cleanup to
        pause the client, so the reactor remains clean between tests.
        """
        self.primary_key = int(time.time() * 1000)  # milliseconds
        client.resume()
        self.addCleanup(client.pause)

    def test_none(self):
        """
        Marshal and unmarshal None.
        """
        def check_result(result):
            self.assertEqual(result, [{"ascii_type": None}])

        d = execute_insert("test", "ascii_type", None, self.primary_key)
        d.addCallback(lambda _: execute_select("test", "ascii_type",
                                               self.primary_key))
        return d.addCallback(check_result)


def make_test_method(cql_type, data):
    """
    Given the key and value from `cql3_types` above, dynamically add a test
    case for marshalling and unmarshalling.
    """
    shortname = short_name(cql_type)
    column_name = "{0}_type".format(shortname)
    test_case_name = 'test_{0}'.format(shortname)
    table_name = "test_counter" if cql_type == "counter" else "test"

    def f(self):
        """Marshal and unmarshal a {0}.""".format(cql_type)
        def check_result(result):
            self.assertEqual(result, [{column_name: data["expected"]}])

        d = execute_insert(table_name, column_name, data["insert"],
                           self.primary_key)
        d.addCallback(lambda _: execute_select(table_name, column_name,
                                               self.primary_key))
        return d.addCallback(check_result)

    f.__name__ = test_case_name
    setattr(UnmarshallingTestCase, test_case_name, f)


# Dynamically add test methods to the UnmarshallingTestCase class.
for _type, data in cql3_types.iteritems():
    if not data.get('supported', True):
        continue
    make_test_method(_type, data)


if not _endpoint:
    UnmarshallingTestCase.skip = "No Cassandra endpoint provided."
