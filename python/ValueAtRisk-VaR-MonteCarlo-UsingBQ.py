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


# # Setup tickers array

# In[ ]:


tickers = ['ULVR.L','VOD.L', 'STAN.L', 'HSBA.L', 'CCH.L', 'BARC.L']
years = 20
current_timestamp = datetime.now(pytz.utc)
end_date = current_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
print(end_date)
days = 20
venue = 'LSE'
batch = random.randint(0, 10000)
simulations = 10000

# # Define a function to fetch name value from env var or command line argument
def get_value(name, default=None):
    # Try env var first
    value = os.environ.get(name)  
    if value is not None:
        return value

    # Then command line argument
    parser = argparse.ArgumentParser() 
    parser.add_argument(f"--{name}", default=default)
    args = parser.parse_args()
    
    return vars(args)[name]

days = get_value("DAYS", days)
print(f"Forecasting for {days} days")
years = get_value("YEARS", years)
print(f"Analyzing data of last {years} years")
start_date = current_timestamp - pd.Timedelta(days= (years * 365))
start_date = start_date.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
print(start_date)
batch = get_value("BATCH_JOB_ID", batch)
print(f"Running task for Batch job ID: {batch}")
simulations = get_value("SIMULATIONS_PER_TASK", simulations)
print(f"Starging {simulations} per task")
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
  print(close_df[ticker])


print(close_df)


# # Calculate Daily Log Returns
# Calculate the daily log returns for each stock in our portfolio and drop any missing values.

# In[ ]:


### Calculate the daily log returns and drop any NAs
log_returns = np.log(close_df/close_df.shift(1))
log_returns  = log_returns.dropna()

print(log_returns)

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
print(cov_matrix)


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
scenarioReturn = []

for i in range(simulations):
    z_score = random_z_score()
    scenarioReturn.append(scenario_gain_loss(portfolio_value, portfolio_std_dev, z_score, days))


# # Populate the simulation output to GCS
# The HPC can run millions of simulations simultaenously, each worker pumping output of
# 
# ```
# scenarioReturn
# ```
# to the GCS bucket.
# 
# You need a job that will ingest the GCS bucket data to BQ.
# The VaR app can use the cumulative BQ table published by the HPC cluster and computes VaR on top of it.
# You can use any visualization tool to plot the histogram.
# 

# In[11]:


# This batch ID will be replaced by HPC batch job ID



# Create a GCS storage client
storage_client = storage.Client()

# Specify the project ID and bucket name
project_id = 'duet-1'
bucket_name = 'duet-1-refinitiv-var-mc--simulations'

# Create a directory named 'my-directory' in the bucket
directory_name = "batch-" + str(batch) + "/"

# Create a new folder
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(directory_name)
blob.upload_from_string(' ')

# Create a file
csv_file_name = str(batch) + '-1.csv'
with open(csv_file_name, 'w') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['BATCH', 'VENUE', 'PORTFOLIO', 'PORTFOLIO_VALUE', 'START_DATE', 'END_DATE', 'DAYS', 'RETURN'])
    portfolio = ','.join(tickers)
    for returnValue in scenarioReturn:
        row = [batch, venue, portfolio, portfolio_value, start_date, end_date, days, returnValue]
        csv_writer.writerow(row)
    csvfile.close()

# Upload the file to the bucket
gcs_csv_path = directory_name + csv_file_name
blob = bucket.blob(gcs_csv_path)
blob.upload_from_filename(csv_file_name)

# Verify the directory creation
print(bucket.list_blobs())



# # Copy data from GCS to BQ

# In[12]:


client = bigquery.Client()
table_name = 'duet-1.VaR.VaR-RETURNS'

# Construct the URI for the CSV file in the GCS bucket
gcs_uri = f'gs://{bucket_name}/{directory_name}{csv_file_name}'


#BATCH, VENUE,
  # PORTFOLIO,
  # PORTFOLIO_VALUE,
  # START_DATE,
  # END_DATE,
  # DAYS,
  # RETURN

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("BATCH", "STRING"),
        bigquery.SchemaField("VENUE", "STRING"),
        bigquery.SchemaField("PORTFOLIO", "STRING"),
        bigquery.SchemaField("PORTFOLIO_VALUE", "FLOAT64"),
        bigquery.SchemaField("START_DATE", "TIMESTAMP"),
        bigquery.SchemaField("END_DATE", "TIMESTAMP"),
        bigquery.SchemaField("DAYS", "INT64"),
        bigquery.SchemaField("RETURN", "FLOAT64")
    ],
    skip_leading_rows=1,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
    # write_disposition = bigquery.WriteDisposition().WRITE_TRUNCATE
)


# Load the CSV file into the BigQuery table
load_job = client.load_table_from_uri(gcs_uri, table_name, job_config=job_config)

# Wait for the load job to complete
load_job.result()  # Returns `None` when the load completes

# Check if the load job had errors
if load_job.errors:
    print(load_job.errors)
else:
    print('Data loaded successfully')


# # Calculate Value at Risk (VaR)
# Specify a confidence interval of 99% and calculate Value at Risk (VaR) using the results of our simulations.

# In[13]:


### Specify a confidence interval and calculate the Value at Risk (VaR)
confidence_interval = 0.99
VaR = -np.percentile(scenarioReturn, 100 * (1 - confidence_interval))
print(VaR)
