#!/usr/bin/env python

################################################################
# Author : Vysali Kallepalli
# Script : topic_clean.py
################################################################

from confluent_kafka import Consumer
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
import argparse


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("topic", help="Kafka topic to consume from")
    parser.add_argument('config_file', type=FileType('r'))
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()

    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    # Create Consumer instance
    consumer = Consumer(config)

    # Subscribe to topic
    consumer.subscribe([args.topic])

    # Process messages
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting for message...")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                print(
                    f"Read and discarded record with key: {record_key} and value: {record_value}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
