#!/usr/bin/env python

# Copyright KOLIBERO under one or more contributor license agreements.  
# KOLIBERO licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import uuid
import argparse
import requests
import time
from datetime import datetime
from kafka import KafkaProducer

BTC_TICKER_URL = "https://www.bitstamp.net/api/v2/ticker/btcusd"
# sec
INTERVAL = 300

# Normalization base
btc_norm = {
  "USD" : 1268.86
  }

def create_msg():
#  msg = {"target":"BTC", "timestamp":int(time.time() * 1000)}
  r = requests.get(BTC_TICKER_URL)
  msg = r.json()
  secs = msg.get("timestamp")
  millis = int(secs)*1000
  msg["timestamp"] = millis
  msg["topic"] = "BTCUSD"
  last = float(msg.get("last"))
  last_n = last/btc_norm.get("USD")
  msg["last_n"] = last_n
  return msg

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description="USD/BTC 2 Kafka")
  parser.add_argument('--kafka_bootstrap_srvs', default="localhost:9092")
  parser.add_argument('--kafka_group_id', default="Rate2Kafka2")
  parser.add_argument('--kafka_target_topic', default="btc_rates2")

  args = parser.parse_args()
  print "Kafka boostrap servers",args.kafka_bootstrap_srvs
  print "Kafka group id",args.kafka_group_id
  print "Kafka target topic",args.kafka_target_topic

  producer = KafkaProducer(bootstrap_servers=args.kafka_bootstrap_srvs,value_serializer=lambda v: json.dumps(v).encode('utf-8'))

  while True:
    msgj = create_msg()
    print msgj
    producer.send(args.kafka_target_topic,msgj)
    time.sleep(INTERVAL)
