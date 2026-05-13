from confluent_kafka import Producer
import json
import time
import random

conf = {
    'bootstrap.servers': 'PASTE_YOUR_BOOTSTRAP_SERVERS_HERE',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms':   'PLAIN',
    'sasl.username':     'PASTE_YOUR_API_KEY_HERE',
    'sasl.password':     'PASTE_YOUR_API_SECRET_KEY_HERE'
}

producer = Producer(conf)

cities = ['Mumbai', 'Delhi', 'Bangalore', 'Hyderabad', 'Chennai']
items  = ['Laptop', 'Phone', 'Shoes', 'Watch', 'Headphones']

def delivery_report(err, msg):
    if err:
        print(f'❌ Failed: {err}')
    else:
        print(f'✅ Sent to partition {msg.partition()} | offset {msg.offset()}')

print("Sending orders... press Ctrl+C to stop\n")

try:
    order_id = 1
    while True:
        order = {
            'order_id':  order_id,
            'city':      random.choice(cities),
            'item':      random.choice(items),
            'amount':    round(random.uniform(199, 9999), 2),
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }

        producer.produce(
            topic    = 'orders',
            key      = str(order['order_id']),
            value    = json.dumps(order),
            callback = delivery_report
        )

        producer.poll(0)
        print(f"Order #{order_id}: {order['item']} from {order['city']} ₹{order['amount']}")
        order_id += 1
        time.sleep(2)

except KeyboardInterrupt:
    print('\nStopping...')
    producer.flush()
    print('Done!')