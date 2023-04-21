#!/usr/bin/env python

################################################################
# Author : Vysali Kallepalli
# Script : consumer_multopic.py
################################################################

import sys
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])
    config['isolation.level'] = 'read_committed'

    # Create Consumer instance
    consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Subscribe to topics
    topics = ["purchases", "topic_2"]
    consumer.subscribe(topics, on_assign=reset_offset)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            elif msg.headers() and msg.headers()[0][0] == "transaction_status" and msg.headers()[0][1] != b"committed":
                # Skip any messages that have transactional headers and are not committed.
                print("Skipping uncommitted message from topic {topic}: key = {key:12} value = {value:12}".format(
                    topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
            else:
                print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                    topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
