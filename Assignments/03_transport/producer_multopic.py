#!/usr/bin/env python

################################################################
# Author : Vysali Kallepalli
# Script : producer_multopic.py
################################################################

import sys
import json
import random
import time
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer

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
    producer = Producer({
        **config,
        'transactional.id': 'my-transactional-id'
    })

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
        # If no key field is specified, use the first field in the data.
        key_field = list(data[0].keys())[0]

    # Start transaction
    producer.init_transactions()
    producer.begin_transaction()

    try:
        # Produce two messages to the first topic "purchases".
        topic = "purchases"
        for message in data[:2]:
            key = message[key_field]
            value = json.dumps(message)
            producer.produce(topic, key=key.encode(
                'utf-8'), value=value.encode('utf-8'), callback=delivery_callback)

        # Produce two messages to the second topic "topic_2".
        topic_2 = "topic_2"
        for message in data[2:4]:
            key = message[key_field]
            value = json.dumps(message)
            producer.produce(topic_2, key=key.encode(
                'utf-8'), value=value.encode('utf-8'), callback=delivery_callback)

        # Block until the messages are sent.
        producer.flush()

        # Waiting for 2 seconds
        print("Waiting for the transaction to be committed/aborted")
        time.sleep(2)

        # Decide whether to commit or abort the transaction randomly
        commit = random.choice([True, False])
        if commit:
            producer.commit_transaction()
            print("Transaction committed successfully!")
        else:
            producer.abort_transaction()
            print("Transaction aborted!")

    except Exception as e:
        # Abort transaction if an exception occurs
        producer.abort_transaction()
        print("Transaction aborted due to an error:", e)
        sys.exit(1)

    # # Block until the messages are sent.
    # producer.flush()
