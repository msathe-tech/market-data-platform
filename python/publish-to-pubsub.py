"""Publishes multiple messages to a Pub/Sub topic with an error handler."""
from concurrent import futures
from google.cloud import pubsub_v1
from typing import Callable
import json
from datetime import datetime
import random
import os
import pytz

tickersList = 'ULVR.L,VOD.L,STAN.L,HSBA.L,CCH.L,BARC.L'
years = 20
current_timestamp = datetime.now(pytz.utc)
end_date = current_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
print(end_date)
start_date = current_timestamp - pd.Timedelta(days= (years * 365))
start_date = start_date.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
days = 20
venue = 'LSE'
batch = random.randint(0, 10000)
simulations = 2
portfolio_value = 1000000

value = os.environ.get("BATCH_JOB_ID")  
if value is not None:
    batch = value
print(f"Running task for Batch job ID: {batch}")

# TODO(developer)
project_id = "duet-1"
topic_id = "simple-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)
publish_futures = []

def get_callback(
    publish_future: pubsub_v1.publisher.futures.Future, data: str
) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
    def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
        try:
            # Wait 60 seconds for the publish call to succeed.
            print(publish_future.result(timeout=60))
        except futures.TimeoutError:
            print(f"Publishing {data} timed out.")

    return callback



for i in range(10):
    json_data = {}
    json_data['batch'] = batch
    json_data['venue'] = venue
    json_data['portfolio'] = tickersList
    json_data['portfolio_value'] = portfolio_value
    json_data['start_date'] = start_date
    json_data['end_date'] = end_date
    json_data['days'] = days
    json_data['return'] = i 
    json_string = json.dumps(json_data)
    print(json_string)
    # When you publish a message, the client returns a future.
    publish_future = publisher.publish(topic_path, json_string.encode("utf-8"))
    # Non-blocking. Publish failures are handled in the callback function.
    publish_future.add_done_callback(get_callback(publish_future, json_string))
    publish_futures.append(publish_future)

# Wait for all the publish futures to resolve before exiting.
futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

print(f"Published messages with error handler to {topic_path}.")