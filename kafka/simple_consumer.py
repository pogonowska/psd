import json
from kafka import KafkaConsumer
import argparse

def main(args):
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_server,
        auto_offset_reset=args.auto_offset_reset
        )
    
    for message in consumer:
        print(json.loads(message.value))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap-server', default='localhost:9092')
    parser.add_argument('--topic', default='transactions')
    parser.add_argument('--auto_offset_reset', default='earliest')
    args = parser.parse_args()
    main(args)