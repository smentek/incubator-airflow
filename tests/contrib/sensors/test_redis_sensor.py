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

from mock import patch

from airflow import DAG
from airflow import configuration
from airflow.contrib.sensors.redis_key_sensor import RedisKeySensor
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestRedisSensor(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }

        self.dag = DAG('test_dag_id', default_args=args)
        self.sensor = RedisKeySensor(
            task_id='test_task',
            redis_conn_id='redis_default',
            dag=self.dag,
            key='test_key'
        )

    @patch("airflow.contrib.sensors.redis_key_sensor.RedisHook")
    def test_poke(self, RedisHook):
        RedisHook.return_value.get_conn.return_value.exists.side_effect = [True, False]
        self.assertTrue(self.sensor.poke(None), "Key exists on first call.")
        RedisHook.assert_called_once_with('redis_default')
        RedisHook.return_value.get_conn.assert_called_once_with()
        RedisHook.return_value.get_conn.return_value.exists.assert_called_once_with('test_key')
        self.assertFalse(self.sensor.poke(None), "Key does NOT exists on second call.")

    @patch("airflow.contrib.hooks.redis_hook.StrictRedis")
    @patch('airflow.contrib.hooks.redis_hook.RedisHook.get_connection')
    def test_existing_key_called(self, redis_hook_get_connection, StrictRedisMock):
        self.sensor.run(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            ignore_ti_state=True
        )

        StrictRedisMock.return_value.exists.assert_called_once_with('test_key')


if __name__ == '__main__':
    unittest.main()
