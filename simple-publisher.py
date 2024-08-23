# simple-publisher.py
#
# Simple version of Hive Podping watcher that pushes new urls
# to an MQTT broker.
# The only external library needed is "beem" - pip install beem
# Beem is the official Hive accessing library for Python.
#
# Version 1.1

from threading import Thread
import time
import json
import argparse
import logging
import sys
import os
from datetime import datetime, timedelta
from typing import Set
import json
import pika

import beem
from beem.account import Account
from beem.blockchain import Blockchain


WATCHED_OPERATION_IDS = ["podping", "pp_"]
LIVETEST_WATCHED_OPERATION_IDS = ["podping-livetest", "pplt_"]

last_message_time = 0

def get_allowed_accounts(acc_name="podping") -> Set[str]:
    """get a list of all accounts allowed to post by acc_name (podping)
    and only react to these accounts"""

    # This is giving an error if I don't specify api server exactly.
    # TODO reported as Issue on Beem library https://github.com/holgern/beem/issues/301
    h = beem.Hive(node="https://api.hive.blog")

    master_account = Account(acc_name, blockchain_instance=h, lazy=True)

    return set(master_account.get_following())


def allowed_op_id(operation_id: str) -> bool:
    """Checks if the operation_id is in the allowed list"""
    for id in WATCHED_OPERATION_IDS:
        if operation_id.startswith(id):
            return True


def block_num_back_in_minutes(blockchain: Blockchain, m: int) -> int:
    """Takes in a time in minutes and returns a block_number to start watching from"""
    back_time = datetime.utcnow() - timedelta(minutes=m)
    block_num = blockchain.get_estimated_block_num(back_time)
    return block_num
    
def start(client: pika.BlockingConnection, args):
    try:
        """Outputs URLs as they appear on the Hive Podping stream"""
        allowed_accounts = get_allowed_accounts()
        hive = beem.Hive()
        blockchain = Blockchain(mode="head", blockchain_instance=hive)

        # Look back 15 minutes
        start_block = block_num_back_in_minutes(blockchain, 15)

        # If you want instant confirmation, you need to instantiate
        # class:beem.blockchain.Blockchain with mode="head",
        # otherwise, the call will wait until confirmed in an irreversible block.
        # noinspection PyTypeChecker
        # Filter only for "custom_json" operations on Hive.
        stream = blockchain.stream(
            opNames=["custom_json"], raw_ops=False, threading=False, start=start_block
        )

        for post in stream:
            # Filter only on post ID from the list above.
            if allowed_op_id(post["id"]):
                # Filter by the accounts we have authorised to podping
                if set(post["required_posting_auths"]) & allowed_accounts:
                    data = json.loads(post.get("json"))
                    if data.get("iris"):
                        publish(client, args, [], data.get("iris"))
                    elif data.get("urls"):
                        publish(client, args, data.get("urls"), [])
                    elif data.get("url"):
                        publish(client, args, [data.get("url")], [])
    except Exception as ex:
            logging.error(f"Error: {ex}", exc_info=True)
            sys.exit(1)
            
def publish(client: pika.BlockingConnection, args, urls, iris):
    global last_message_time
    
    print(f"Publishing {urls} {iris}")
    last_message_time = time.time()    
    channel = client.channel()
    response = channel.basic_publish(exchange=args.exchange, routing_key=args.key,
                      body=json.dumps({"urls": urls, "iris": iris, "xurls": []}))

def monitor():
    while True:
        global last_message_time
        time.sleep(60)      
        time_last_message = (time.time() - last_message_time)
        if time_last_message > 300:
            print(f"No messages for {time_last_message} seconds. Exiting.")
            os._exit()
        else:
            print(f"Last message was {time_last_message} seconds ago.")

def declare_exchange(client: pika.BlockingConnection, args):
    channel = client.channel()

    # Declare a durable exchange
    channel.exchange_declare(
        exchange=args.exchange,
        exchange_type='direct',
        durable=True
    )

    # Declare a durable queue
    channel.queue_declare(
        queue=args.key,
        durable=True  # Durable queue
    )

    # Bind the queue to the exchange with a routing key
    channel.queue_bind(
        queue=args.key,
        exchange=args.exchange,
        routing_key=args.key
    )
              
def main():
    parser = argparse.ArgumentParser(description='Publish hive messages')
    parser.add_argument('--host', default="127.0.0.1", help='host')
    parser.add_argument('--port', default=5672, help='port')
    parser.add_argument('--exchange', required=True, help='topic exchange')
    parser.add_argument('--key', required=True, help='topic routing key')
    parser.add_argument('--username', default = 'rmq_user', help='username')
    parser.add_argument('--password', default = 'rmq_password', help='password')
    args = parser.parse_args()
    
    def on_connect(channel: pika.BlockingConnection, args):
        global last_message_time
        
        last_message_time = time.time()        
        watcher = Thread(target = monitor)
        watcher.start()
        
        start(channel, args)
        
    client =  pika.BlockingConnection(
        pika.ConnectionParameters(
            host=args.host,
            port=args.port,
            virtual_host='/',            
            credentials=pika.PlainCredentials(args.username, args.password)
            ))
    
    declare_exchange(client, args)
    on_connect(client, args)    

if __name__ == "__main__":
    try:
        client = main()
    except KeyboardInterrupt:
        logging.info("Terminated with Ctrl-C")
        sys.exit(1)
    except Exception as ex:
        logging.error(f"Error: {ex}", exc_info=True)
        sys.exit(1)
