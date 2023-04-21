#!/usr/bin/env python

################################################################
# Author : Vysali Kallepalli
# Script : updated_random_producer.py
################################################################

import sys
import json
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
import random

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('data_file', type=FileType('r'))
    parser.add_argument('--key-field', default=None,
                        help='Field to use as message key')
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    producer = Producer(config)

    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    # Read data from the JSON file.
    data = json.load(args.data_file)

    if args.key_field is not None:
        key_field = args.key_field
    else:
        key_field = list(data[0].keys())[0]

    # Produce each message in the data.
    topic = "purchases"
    for message in data:
        key = str(random.randint(1, 5))
        value = json.dumps(message)
        producer.produce(topic, key=key.encode(
            'utf-8'), value=value.encode('utf-8'), callback=delivery_callback)

    # Block until the messages are sent.
    producer.flush()
