#!/usr/bin/env python
# coding: utf-8

# # Import libraries

# In[ ]:


import numpy as np
import pandas as pd
import datetime as dt
from datetime import datetime
import random
import csv
import pytz
import os
import argparse
from google.cloud import storage
from google.cloud import bigquery
import json
from google.cloud import pubsub_v1
from concurrent import futures
from typing import Callable


# # Setup tickers array

# In[ ]:


tickersList = 'ULVR.L,VOD.L'
years = 1
current_timestamp = datetime.now(pytz.utc)
end_date = current_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
print(end_date)
days = 9
venue = 'LSE'
batch = random.randint(0, 10000)
simulations = 10000

parser = argparse.ArgumentParser() 
parser.add_argument(f"--DAYS", type=int, default=days)
parser.add_argument(f"--YEARS", type=int, default=years)
parser.add_argument(f"--SIMULATIONS_PER_TASK", type=int, default=simulations)
parser.add_argument(f"--TICKERS", type=str, default=tickersList)

args = parser.parse_args()


# Split tickers string into list  
tickers = args.TICKERS.split(',') 

print(f"Portfolio = {tickers}")

days = args.DAYS
years = args.YEARS

print(f"Forecasting for {days} days")
print(f"Analyzing data of last {years} years")
# Hardcoding to 120 days for demo
start_date = current_timestamp - pd.Timedelta(days= (120))
start_date = start_date.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
print(start_date)
value = os.environ.get("BATCH_JOB_ID")  
if value is not None:
    batch = value
print(f"Running task for Batch job ID: {batch}")

simulations = args.SIMULATIONS_PER_TASK
print(f"Starting {simulations} per task")
# # Use BigQuery SQL to populate dataframe

# In[ ]:


close_df = pd.DataFrame()

for ticker in tickers:
  sql = f'''WITH subquery AS (
    SELECT *,
          ROW_NUMBER() OVER (PARTITION BY RIC,  DATE(Date_Time) ORDER BY Date_Time DESC) AS row_num
    FROM `LSE_NORMALISED.LSE_NORMALISED`
    WHERE RIC IN ('{ticker}')
    AND (Date_Time BETWEEN TIMESTAMP('{start_date}') AND TIMESTAMP('{end_date}'))
    AND Type="Trade"
    AND Volume >0
    AND Price >0
    AND RIGHT(REGEXP_EXTRACT(Qualifiers, r";(.*)\[MMT_CLASS\]"),14) LIKE "12%"
  )
  SELECT FORMAT_DATETIME('%Y-%m-%d', Date_Time) AS Date, RIC, Price
  FROM subquery
  WHERE row_num = 1
  ORDER BY Date_Time ASC;
  '''
  #print(sql)
  data = pd.read_gbq(sql)
  data.set_index('Date', inplace=True)
  close_df[ticker] = data['Price'].astype(float)
  print(f"Retrieved data for {ticker}")
  #print(close_df[ticker])


#print(close_df)


# # Calculate Daily Log Returns
# Calculate the daily log returns for each stock in our portfolio and drop any missing values.

# In[ ]:


### Calculate the daily log returns and drop any NAs
log_returns = np.log(close_df/close_df.shift(1))
log_returns  = log_returns.dropna()

print("Computed log returns ")
#print(log_returns)

log_returns


# # Define Functions to Calculate Portfolio Expected Return and Standard Deviation
# Define two functions, expected_return() and standard_deviation(), which will be used to calculate the expected return and standard deviation of our portfolio, respectively.

# In[ ]:


### Create a function that will be used to calculate portfolio expected return
### We are assuming that future returns are based on past returns, which is not a reliable assumption.
def expected_return(weights, log_returns):
    return np.sum(log_returns.mean()*weights)

### Create a function that will be used to calculate portfolio standard deviation
def standard_deviation (weights, cov_matrix):
    variance = weights.T @ cov_matrix @ weights
    return np.sqrt(variance)


# # Create a Covariance Matrix
# Create a covariance matrix for all the securities in our portfolio using the daily log returns.

# In[ ]:


### Create a covariance matrix for all the securities
cov_matrix = log_returns.cov()
print("Computed covariance matrix")
#print(cov_matrix)


# # Calculate Portfolio Expected Return and Standard Deviation
# Create an equally weighted portfolio and calculate the portfolio’s expected return and standard deviation using the functions we defined earlier.

# In[ ]:


### Create an equally weighted portfolio and find total portfolio expected return and standard deviation
portfolio_value = 1000000
weights = np.array([1/len(tickers)]*len(tickers))
portfolio_expected_return = expected_return(weights, log_returns)
portfolio_std_dev = standard_deviation (weights, cov_matrix)


# # Define Functions for Monte Carlo Simulation
# Define two functions: random_z_score() and scenario_gain_loss(). The first function generates a random Z-score based on a normal distribution, and the second function calculates the gain or loss for a given scenario.
# 

# In[ ]:


def random_z_score():
    return np.random.normal(0, 1)

### Create a function to calculate scenarioGainLoss
def scenario_gain_loss(portfolio_value, portfolio_std_dev, z_score, days):
    return portfolio_value * portfolio_expected_return * days + portfolio_value * portfolio_std_dev * z_score * np.sqrt(days)


# # Run Monte Carlo Simulation
# Run 10,000 Monte Carlo simulations, calculating the scenario gain/loss for each simulation and storing the results in a list.

# In[ ]:


### Run simulations
# scenarioReturn = []

data = ['BATCH', 'VENUE', 'PORTFOLIO', 'PORTFOLIO_VALUE', 'START_DATE', 'END_DATE', 'DAYS', 'RETURN']

# Create an empty dictionary to store the data
json_data = {}
# Publish to PubSub
publisher = pubsub_v1.PublisherClient()

task_index = os.environ.get("BATCH_TASK_INDEX")
print(f"Current task ID: {task_index}")

project_id = "duet-1"
topic_id = "var-returns"

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


for i in range(simulations):
    json_data = {}
    z_score = random_z_score()
    returnValue = scenario_gain_loss(portfolio_value, portfolio_std_dev, z_score, days)
    json_data['batch'] = batch
    json_data['venue'] = venue
    json_data['portfolio'] = tickersList
    json_data['portfolio_value'] = portfolio_value
    json_data['start_date'] = start_date
    json_data['end_date'] = end_date
    json_data['days'] = days
    json_data['return'] = returnValue
    json_string = json.dumps(json_data)
    print(json_string)
    publish_future = publisher.publish(topic_path, json_string.encode("utf-8"))
    # Non-blocking. Publish failures are handled in the callback function.
    publish_future.add_done_callback(get_callback(publish_future, json_string))
    publish_futures.append(publish_future)
    # scenarioReturn.append(returnValue)

# Wait for all the publish futures to resolve before exiting.
futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)
print(f"Published messages with error handler to {topic_path}.")