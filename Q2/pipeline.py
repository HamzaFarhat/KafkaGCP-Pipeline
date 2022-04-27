#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#Hamza Farhat Ali-100657374
"""A word-counting workflow. MILESTONE 4"""

import argparse
import logging
import json
import math

import apache_beam as beam
from apache_beam.io import ReadFromText, ReadFromPubSub
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from kafka import KafkaProducer


class ConvertFn(beam.DoFn):
    def process(self, element):
        element['pressure'] = element['pressure']/6.895
        element['temperature'] = element['temperature']*1.8 +32
        return element

class RiskFn(beam.DoFn):
    def process(self, element):
        if element['humidity'] >90:
            element['risk'] = 1
        if element['pressure']>=0.2 and element['humidity']<=90:
            element['risk'] = 1- math.pow( 2.71828, 0.2-element['pressure'])
        if element['temperature'] < 0.2 and element['humidity'] <=90:
            element['risk'] = min(1, element['temperature']/300)

        return element


class ProduceKafkaMessage(beam.DoFn):
    def __init__(self, topic, servers, *args, **kwargs):
        beam.DoFn.__init__(self, *args, **kwargs)
        self.topic=topic
        self.servers=servers

    def start_bundle(self):
        self._producer = KafkaProducer(**self.servers)

    def finish_bundle(self):
        self._producer.close()

    def process(self, element):
        try:
            self._producer.send(self.topic, element[1], key=element[0])
            yield element
        except Exception as e:
            raise e

def filterSensor(x):
    return x['temperature'] != 'none' and x['humidity'] != 'none' and x['pressure'] != 'none'

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', required=True,
                        help='topic to input from')
    parser.add_argument('--output', dest='output', required=True,
                        help='Topic to out to')
    # parser.add_argument('--source', dest='source', required=True,
    #                     help='Data source location (text|mysql|kafka).')
    known_args, pipeline_args = parser.parse_known_args(argv)




if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
