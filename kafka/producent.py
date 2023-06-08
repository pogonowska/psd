import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer
from card_user_generator import client_card_generator
import argparse

user_card_pairs = client_card_generator()

# Messages will be serialized as JSON
def serializer(message):
    return json.dumps(message).encode('utf-8')

def generate_transactions(pair_number) -> dict:
    user_card = user_card_pairs[pair_number]
    transaction_value = round(random.uniform(0, min(500, user_card[3])),2)
    user_card_pairs[pair_number][3] -= transaction_value
    user_card = user_card_pairs[pair_number]
    return {
        'card_id': user_card[1],
        'user_id': user_card[0],
        'latitude': round(random.uniform(user_card[2][0]-0.4, user_card[2][0]+0.4), 2), 
        'longitude': round(random.uniform(user_card[2][1]-0.4, user_card[2][1]+0.4), 2),
        'transaction_value': transaction_value,
        'account_limit': user_card[3],
        }

def generate_transactions_change_location(pair_number) -> dict:
    user_card = user_card_pairs[pair_number]
    transaction_value = round(random.uniform(0, min(500, user_card[3])),2)
    user_card_pairs[pair_number][3] -= transaction_value
    user_card = user_card_pairs[pair_number]
    return {
        'card_id': user_card[1],
        'user_id': user_card[0],
        'latitude': round(random.uniform(0, 90), 2),
        'longitude': round(random.uniform(0, 180), 2),
        'transaction_value': transaction_value,
        'account_limit': user_card[3],
        }

def generate_large_transactions(pair_number) -> dict:
    user_card = user_card_pairs[pair_number]
    if user_card[3] > 1000:
        transaction_value = round(random.uniform(1000, user_card[3]),2)
    else:
        transaction_value = round(random.uniform(0, min(500, user_card[3])),2)
    user_card_pairs[pair_number][3] -= transaction_value
    user_card = user_card_pairs[pair_number]
    return {
        'card_id': user_card[1],
        'user_id': user_card[0],
        'latitude': round(random.uniform(user_card[2][0]-0.4, user_card[2][0]+0.4), 2),
        'longitude': round(random.uniform(user_card[2][1]-0.4, user_card[2][1]+0.4), 2),
        'transaction_value': transaction_value,
        'account_limit': user_card[3],
        }

def main(args):
    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap_server],
        value_serializer=serializer
        )
    scenarios = ['normal', 'too_often', 'large', 'strange_location']

    while True:
        scenario = random.choice(scenarios)
        pair_number = random.randint(0, 999)
        if scenario == 'normal':
            message = generate_transactions(pair_number)
            print(f'Producing transaction @ {datetime.now()} | Message = {str(message)}')
            producer.send(args.topic, message)

            time_to_sleep = random.randint(2, 5)
            time.sleep(time_to_sleep)

        if scenario == 'too_often':
            for i in range(4):
                message = generate_transactions(pair_number)
                print(f'Producing wrong transaction, type: too_often @ {datetime.now()} | Message = {str(message)}')
                producer.send(args.topic, message)

                time.sleep(1)

        if scenario == 'large':
            message = generate_large_transactions(pair_number)
            print(f'Producing wrong transaction, type: large @ {datetime.now()} | Message = {str(message)}')
            producer.send(args.topic, message)

            time_to_sleep = random.randint(2, 5)
            time.sleep(time_to_sleep)

        if scenario == 'strange_location':
            message = generate_transactions_change_location(pair_number)
            print(f'Producing wrong transaction, type: strange_location @ {datetime.now()} | Message = {str(message)}')
            producer.send(args.topic, message)

            time_to_sleep = random.randint(2, 5)
            time.sleep(time_to_sleep)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap-server', default='localhost:9092')
    parser.add_argument('--topic', default='transactions')
    args = parser.parse_args()
    main(args)
    