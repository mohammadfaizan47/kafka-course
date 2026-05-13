from confluent_kafka import Consumer
import json

conf = {
    'bootstrap.servers': 'PASTE_YOUR_BOOTSTRAP_SERVERS_HERE',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms':   'PLAIN',
    'sasl.username':     'PASTE_YOUR_API_KEY_HERE',
    'sasl.password':     'PASTE_YOUR_API_SECRET_KEY_HERE',
    'group.id':          'orders-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['orders'])

print("Reading orders... press Ctrl+C to stop\n")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        order = json.loads(msg.value().decode('utf-8'))

        print(f"Partition: {msg.partition()} | "
              f"Offset: {msg.offset()} | "
              f"Order #{order['order_id']} | "
              f"{order['item']} from {order['city']} | "
              f"₹{order['amount']}")

except KeyboardInterrupt:
    print('\nStopping...')
finally:
    consumer.close()
    print('Consumer closed.')