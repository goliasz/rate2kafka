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

#USD2BTC_URL = "https://blockchain.info/tobtc?currency=USD&value=1"
BTC_TICKER_URL = "https://blockchain.info/ticker"
# sec
INTERVAL = 600

def create_msg():
  msg = {"target":"BTC", "timestamp":int(time.time() * 1000)}
  r = requests.get(BTC_TICKER_URL)
  rj = r.json()
  for i in rj.items():
    #print i[0],i[1].get("last")
    msg[i[0]] = i[1].get("last")
  return msg  

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description="USD/BTC 2 Kafka")
  parser.add_argument('--kafka_bootstrap_srvs', default="localhost:9092")
  parser.add_argument('--kafka_group_id', default="Rate2Kafka")
  parser.add_argument('--kafka_target_topic', default="btc_rates")

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
