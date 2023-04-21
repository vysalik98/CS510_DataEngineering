#!/usr/bin/env python

################################################################
# Author : Vysali Kallepalli
# Script : consumer_group_1.py
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

    # Creating consumer group id
    config['group.id'] = 'myconsumer_group_1'

    # Create Consumer instance
    consumer = Consumer(config)
    consumer_1 = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Subscribe to topic
    topic = "purchases"
    consumer.subscribe([topic], on_assign=reset_offset)
    consumer_1.subscribe([topic], on_assign=reset_offset)

    # Start consuming
    try:
        while True:
            msg = consumer.poll(1.0)
            msg_1 = consumer_1.poll(1.0)
            if msg is None and msg_1 is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
            elif msg is not None:
                if msg.error():
                    print("ERROR: %s".format(msg.error()))
                else:
                    print("Consumer1 consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                        topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
            elif msg_1 is not None:
                if msg_1.error():
                    print("ERROR: %s".format(msg_1.error()))
                else:
                    print("Consumer2 consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                        topic=msg_1.topic(), key=msg_1.key().decode('utf-8'), value=msg_1.value().decode('utf-8')))
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
        consumer_1.close()
