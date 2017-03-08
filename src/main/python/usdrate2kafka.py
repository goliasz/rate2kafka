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
#BTC_TICKER_URL = "https://blockchain.info/ticker"
# sec
INTERVAL = 600

BASE_URL = "http://free.currencyconverterapi.com/api/v3/convert?compact=ultra&q="

# Normalization base
norm = {
  "USD_PLN" : 4.0785,
  "USD_EUR" : 0.9491268,
  "USD_CHF" : 1.0151,
  "USD_RUB" : 58.635,
  "USD_SEK" : 9.0576,
  "USD_AUD" : 1.3268,
  "USD_JPY" : 114.603,
  "USD_DKK" : 7.0545,
  "USD_GBP" : 0.82297753,
  "USD_UAH" : 26.94,
  "USD_XDR" : 0.74205996
  }

def create_msg(p_pair):
  msg = {"target":"USD", "timestamp":int(time.time() * 1000)}
  r = requests.get(BASE_URL+p_pair)
  rj = r.json()
  for i in rj.items():
    #print i[0],i[1].get("last")
    msg[i[0]] = float(i[1])
    msg[i[0]+"n"] = float(i[1])/norm.get(i[0],1.0)
  return msg  

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description="USD/x 2 Kafka")
  parser.add_argument('--kafka_bootstrap_srvs', default="localhost:9092")
  parser.add_argument('--kafka_target_topic', default="btc_rates")

  args = parser.parse_args()
  print "Kafka boostrap servers",args.kafka_bootstrap_srvs
  print "Kafka target topic",args.kafka_target_topic

  producer = KafkaProducer(bootstrap_servers=args.kafka_bootstrap_srvs,value_serializer=lambda v: json.dumps(v).encode('utf-8'))

  while True:
    for i in norm.items():
      print i[0] 
      msgj = create_msg(i[0])
      print msgj
      producer.send(args.kafka_target_topic,msgj)
    time.sleep(INTERVAL)
