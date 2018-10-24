# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


import unittest
from mock import patch, MagicMock
from airflow import configuration
from airflow.contrib.hooks.redis_hook import RedisHook


class TestRedisHook(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()

    @patch('airflow.contrib.hooks.redis_hook.StrictRedis')
    @patch('airflow.contrib.hooks.redis_hook.RedisHook.get_connection')
    def test_get_conn(self, redis_hook_get_connection_mock, StrictRedisMock):
        HOST = 'localhost'
        PORT = 6379
        PASSWORD = 's3cret!'
        DB = 0

        extra_dejson_mock = MagicMock()
        extra_dejson_mock.get.return_value = DB
        connection_parameters = MagicMock()
        connection_parameters.configure_mock(
            host=HOST,
            port=PORT,
            password=PASSWORD,
            extra_dejson=extra_dejson_mock)
        redis_hook_get_connection_mock.return_value = connection_parameters

        hook = RedisHook(redis_conn_id='redis_default')
        self.assertEqual(hook.redis, None)

        self.assertEqual(hook.host, None, "host initialised as None.")
        self.assertEqual(hook.port, None, "port initialised as None.")
        self.assertEqual(hook.password, None, "password initialised as None.")
        self.assertEqual(hook.db, None, "db initialised as None.")

        self.assertIs(hook.get_conn(), hook.get_conn(), "Connection initialized only if None.")

        StrictRedisMock.assert_called_once_with(
            host=HOST,
            port=PORT,
            password=PASSWORD,
            db=DB,
        )

    @patch('airflow.contrib.hooks.redis_hook.StrictRedis')
    @patch('airflow.contrib.hooks.redis_hook.RedisHook.get_connection')
    def test_get_conn_password_stays_none(self, redis_hook_get_connection_mock, StrictRedisMock):
        HOST = 'localhost'
        PORT = 6379
        PASSWORD = 'None'
        DB = 0

        extra_dejson_mock = MagicMock()
        extra_dejson_mock.get.return_value = DB
        connection_parameters = MagicMock()
        connection_parameters.configure_mock(host=HOST, port=PORT, password=PASSWORD,
                                             extra_dejson=extra_dejson_mock)
        redis_hook_get_connection_mock.return_value = connection_parameters

        hook = RedisHook(redis_conn_id='redis_default')
        hook.get_conn()
        self.assertEqual(hook.password, None)


if __name__ == '__main__':
    unittest.main()
