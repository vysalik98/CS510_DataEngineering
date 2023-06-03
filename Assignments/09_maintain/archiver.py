#!/usr/bin/env python

import sys
import os
import json
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
from google.cloud import storage

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    parser.add_argument('--bucket', required=True, help='GCS bucket name')
    parser.add_argument('--file', required=True, help='File name to store consumed data')
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    # Creating consumer group id
    config['group.id'] = 'consumergrp_2'

    # Create Consumer instance
    consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Subscribe to topic
    topic = "archivetest"
    consumer.subscribe([topic], on_assign=reset_offset)

    # Create a GCS client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(args.bucket)

    # Create a list to store the consumed data
    consumed_data = []

    # Poll for new messages from Kafka, write to local file, and store in GCS.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
            elif msg.error():
                print("ERROR: {}".format(msg.error()))
            else:
                data = {
                    'topic': msg.topic(),
                    'key': msg.key().decode('utf-8'),
                    'value': msg.value().decode('utf-8')
                }
                consumed_data.append(data)

                # Write to local file
                with open(args.file, 'a') as f:
                    json.dump(data, f)
                    f.write('\n')

                print("Event written to file.")

                # Store in GCS
                json_data = json.dumps(data)
                blob_name = os.path.basename(args.file)  
                blob = bucket.blob(blob_name)
                blob.upload_from_string(json_data)

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
