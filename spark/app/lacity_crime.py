#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Used to display all the matplotlib graphs inside the notebook

# Hiding the warnings
import warnings
warnings.filterwarnings('ignore')

# In[2]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, expr, lpad, min, max, substring, hour, concat_ws, to_timestamp, regexp_replace, trim, initcap, datediff, date_trunc, month, year
from datetime import datetime
from pyspark.sql import functions as F
import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.statespace.sarimax import SARIMAX
from google.oauth2 import service_account
import pandas_gbq
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from functools import reduce
from pyspark.sql import DataFrame

# In[3]:


# Initialize Spark session
spark = SparkSession.builder.appName("CrimeDataAnalysis").master("spark://spark:7077").getOrCreate()

# Download JSON data
url = 'https://data.lacity.org/resource/2nrs-mtv8.json?$select=count(*)'
response = requests.get(url)
data = json.loads(response.text)

# Convert data to a Spark DataFrame
crime_count = int(data[0]['count'])
n_crimes = crime_count

print('The number of crimes till date is: ', n_crimes)


# In[4]:


# Initialize Spark session
# spark = SparkSession.builder.appName("CrimeDataAnalysis").config("spark.jars", "hdfs://maskxdc/test/flint-0.6.0.jar").getOrCreate()

# Define API URL
API_URL = "https://data.lacity.org/resource/2nrs-mtv8.json"

def get_count(start_date, end_date):
    query = f"{API_URL}?$select=count(*)&$where=date_rptd between '{start_date}' and '{end_date}'"
    res = requests.get(query)
    if res.status_code != 200:
        raise Exception(f"Return code: {res.status_code}\tReturn Text: {res.content}")

    records_count = int(res.json()[0]['count'])
    return records_count

def get_data(start_date, end_date, limit, batch_num):
    err_corr = int(limit * 0.1)

    offset = (batch_num * limit) - err_corr if batch_num != 0 else 0
    limit = limit + 2 * err_corr
    query = (
        f"{API_URL}?$order=date_rptd ASC&$where=date_rptd between '{start_date}' and '{end_date}'&$limit={limit}&$offset={offset}"
    )
    res = requests.get(query)
    if res.status_code != 200:
        raise Exception(f"Return code: {res.status_code}\tReturn Text: {res.content}")

    data = res.json()
    return spark.createDataFrame(data)

def load_data(start_date, end_date, batch_size=50000, debug=True):
    if debug:
        # Assuming you have a CSV file in the same format as the original code
        return spark.read.csv("/usr/local/spark/resources/data/la_crime_data_10000.csv", header=True, inferSchema=True)

    # Get total count of columns to load
    records_count = get_count(start_date, end_date)
    print(f"Fetching {records_count} records...")

    num_batches = int(records_count / batch_size) + 1

    # Fetch data in batches
    all_data = []
    for batch_num in range(num_batches):
        batch_data = get_data(start_date, end_date, batch_size, batch_num)
        all_data.append(batch_data)

    return reduce(DataFrame.unionAll, all_data).dropDuplicates()


# In[5]:


EXECUTION_DATE = os.getenv("EXECUTION_DATE")

start_date = (datetime.strptime(EXECUTION_DATE[:10], "%Y-%m-%d") - relativedelta(months=1)).strftime("%Y-%m-%d")
end_date = EXECUTION_DATE[:10]

print(f"Start Date: {start_date}, end date: {end_date}")

# crime_data = load_data(start_date, end_date, debug=False)
# crime_data.to_csv('la_crime_data.csv', index=False)
crime_data = load_data(start_date, end_date, debug=False)

# Display the shape of the DataFrame
num_rows = crime_data.count()
num_columns = len(crime_data.columns)
print(f"Number of rows: {num_rows}")
print(f"Number of columns: {num_columns}")


# In[6]:


# Create another DataFrame 'data' and store the content of 'crime_data'
data = crime_data.alias("data")


# In[7]:


def get_shape(df):
    num_rows = df.count()
    num_columns = len(df.columns)
    return num_rows, num_columns

# Usage
num_rows, num_columns = get_shape(data)
print("Shape of the Crimes data DataFrame is:", (num_rows, num_columns))


# In[8]:


# Show the first few rows of the DataFrame
#data.show(5)


# In[9]:


print("The shape before removing duplicates from the dataset is:", (num_rows, num_columns))

# Remove duplicates
data = data.dropDuplicates()

# Check the shape after removing duplicates
num_rows_after = data.count()
print("The shape after removing duplicates from the dataset is:", (num_rows_after, num_columns))


# In[10]:


# Print the schema of the DataFrame
data.printSchema()


# In[11]:


# Calculate the percentage of null values in each column
num_rows = data.count()
null_percentages = [(count(when(col(c).isNull(), c)) / num_rows * 100).alias(c) for c in data.columns]

# Create a DataFrame with the null percentages
null_percentage_df = data.agg(*null_percentages)

# Show the null percentages
# null_percentage_df.show()


# In[12]:


# Convert 'date_rptd' and 'date_occ' columns to datetime
data = data.withColumn("date_rptd", col("date_rptd").cast("timestamp"))
data = data.withColumn("date_occ", col("date_occ").cast("timestamp"))

# Show the DataFrame after conversion
# data.show(5)


# In[13]:


# Convert 'time_occ' column to string
data = data.withColumn("time_occ", col("time_occ").cast("string"))

# Add leading zeros to make it four digits
data = data.withColumn("time_occ", lpad(col("time_occ"), 4, '0'))

# Format 'time_occ' as HH:mm
data = data.withColumn("time_occ", concat_ws(":", substring(col("time_occ"), 1, 2), substring(col("time_occ"), 3, 2)))

# Cast 'time_occ' to timestamp
data = data.withColumn("time_occ", to_timestamp(col("time_occ"), "HH:mm"))


# In[14]:


# Define age groups and labels
age_groups = [
    (col("vict_age") < 1),
    (col("vict_age").between(1, 12)),
    (col("vict_age").between(13, 17)),
    (col("vict_age").between(18, 64)),
    (col("vict_age") >= 65)
]

labels = ['Unknown', 'Child', 'Teenager', 'Adult', 'Old']

# Create new column 'Age Group'
data = data.withColumn("Age Group", when(age_groups[0], labels[0])
                                   .when(age_groups[1], labels[1])
                                   .when(age_groups[2], labels[2])
                                   .when(age_groups[3], labels[3])
                                   .when(age_groups[4], labels[4])
                                   .otherwise("Unknown"))


# In[15]:


# Replace values in 'vict_sex' column
data = data.withColumn("vict_sex", when((col("vict_sex") == 'M'), 'Male')
                                   .when((col("vict_sex") == 'F'), 'Female')
                                   .when((col("vict_sex") == 'X') | (col("vict_sex") == 'H') | (col("vict_sex").isNull()), 'Unknown')
                                   .otherwise(col("vict_sex")))

# Fill null values with 'Data Missing'
data = data.na.fill('Data Missing', subset=["vict_sex"])


# In[16]:


# Get unique values and their counts in 'vict_descent' column
unique_values_counts = data.groupBy("vict_descent").count()

# Show the result
# unique_values_counts.show(truncate=False)


# In[17]:


# Define descent mapping
descent_mapping = {
    'A': 'Other Asian',
    'B': 'Black',
    'C': 'Chinese',
    'D': 'Cambodian',
    'F': 'Filipino',
    'G': 'Guamanian',
    'H': 'Hispanic/Latin/Mexican',
    'I': 'American Indian/Alaskan Native',
    'J': 'Japanese',
    'K': 'Korean',
    'L': 'Laotian',
    'O': 'Other',
    'P': 'Pacific Islander',
    'S': 'Samoan',
    'U': 'Hawaiian',
    'V': 'Vietnamese',
    'W': 'White',
    'X': 'Unknown',
    '-': 'Unknown',
    'Z': 'Asian Indian'
}

print("AFTER DESCENT MAPPING")

# Replace values in 'vict_descent' column
for key, value in descent_mapping.items():
    print(f"INSIDE LOOP WITH KEY: {key}")
    data = data.withColumn("vict_descent", when((col("vict_descent") == key), value).otherwise(col("vict_descent")))

# Fill null values with 'Data Missing'
data = data.na.fill("Data Missing", subset=["vict_descent"])
print("FINISHED NULL VALUE REPLACE WITH 'data missing'")


# In[18]:


# Get unique values and their counts in 'vict_descent' column
unique_values_counts = data.groupBy("vict_descent").count()
print("FINISHED getting unique values in 'vict_descent'")

# Show the result
# unique_values_counts.show(truncate=False)


# In[19]:


# Fill null values in 'premis_desc' column with 'Data Missing'
data = data.na.fill("Data Missing", subset=["premis_desc"])
print("FILL NULL VALUES IN 'PREMIS_DESC' WITH COLUMN WITH 'DATA MISSING'")


# In[20]:


# Get unique values and their counts in 'status_desc' column
unique_values_counts = data.groupBy("status_desc").count()

# Show the result
#unique_values_counts.show(truncate=False)


# In[21]:


# Fill null values in 'weapon_desc' column with 'Data Missing'
data = data.na.fill("Data Missing", subset=["weapon_desc"])


# In[22]:


# Fill missing values in 'cross_street' and 'mocodes' columns with 'Data Missing'
data = data.na.fill("Data Missing", subset=["cross_street", "mocodes"])


# In[23]:


# Rename columns based on the provided mapping
data = data \
    .withColumnRenamed("lat", "Latitude") \
    .withColumnRenamed("lon", "Longitude") \
    .withColumnRenamed("status_desc", "Case Status") \
    .withColumnRenamed("premis_desc", "Crime Location") \
    .withColumnRenamed("vict_descent", "Victim Descent") \
    .withColumnRenamed("vict_sex", "Victim Gender") \
    .withColumnRenamed("weapon_desc", "Weapon Description") \
    .withColumnRenamed("vict_age", "Victim Age") \
    .withColumnRenamed("mocodes", "Modus Operandi") \
    .withColumnRenamed("date_rptd", "Date Reported") \
    .withColumnRenamed("date_occ", "Date Occurred") \
    .withColumnRenamed("time_occ", "Time Occurred") \
    .withColumnRenamed("part_1_2", "Part 1-2") \
    .withColumnRenamed("location", "Street Address") \
    .withColumnRenamed("crm_cd", "Crime Code") \
    .withColumnRenamed("crm_cd_desc", "Crime Description") \
    .withColumnRenamed("area", "Area Code")

data.columns


# In[24]:


# List of columns to drop
columns_to_drop = ['premis_cd', 'crm_cd_1', 'crm_cd_2', 'crm_cd_3', 'crm_cd_4', 'status', 'weapon_used_cd']

# Select columns that are not in the list of columns to drop
data = data.select([column for column in data.columns if column not in columns_to_drop])


# In[25]:


# Replace zeros with None in 'Latitude' and 'Longitude' columns because
# 0s represent location unknow, which are masked for privacy with 0s
# this step is done to avoid any confusion in map plots in Tableau

data = data.withColumn("Latitude", when(col("Latitude") == 0, None).otherwise(col("Latitude")))
data = data.withColumn("Longitude", when(col("Longitude") == 0, None).otherwise(col("Longitude")))


# In[26]:


# Replace underscores with spaces and title-case column names
for column in data.columns:
    new_column = column.replace("_", " ").title()
    data = data.withColumnRenamed(column, new_column)
    
data.columns


# In[27]:


# Function to remove text inside brackets
def remove_text_inside_brackets(sentence):
    return trim(regexp_replace(sentence, '\(.*?\)', ''))

# Apply the function to each row in the 'Crime Description' column
data = data.withColumn('Crime Description', remove_text_inside_brackets(col('Crime Description')))
data = data.withColumn('Crime Description', regexp_replace(col('Crime Description'), ',', '-'))
data = data.withColumn('Crime Description', initcap(trim(col('Crime Description'))))


# In[28]:


# Apply the function to each row in the 'Weapon Description' column
data = data.withColumn('Weapon Description', remove_text_inside_brackets(col('Weapon Description')))
data = data.withColumn('Weapon Description', regexp_replace(col('Weapon Description'), ',', '-'))
data = data.withColumn('Weapon Description', initcap(trim(col('Weapon Description'))))


# In[29]:


# Apply transformations to the 'Crime Location' column
data = data.withColumn('Crime Location', trim(col('Crime Location')))
data = data.withColumn('Crime Location', initcap(col('Crime Location')))


# In[30]:


# Replace asterisks in the 'Crime Location' column
data = data.withColumn('Crime Location', regexp_replace(col('Crime Location'), '\\*', ''))


# In[31]:


# Apply transformations to the 'Street Address' column
data = data.withColumn('Street Address', trim(col('Street Address')))
data = data.withColumn('Street Address', initcap(col('Street Address')))


# In[32]:


# Function to remove extra spaces
def remove_extra_spaces(address_string):
    return regexp_replace(address_string, ' +', ' ')

# Apply the function to the 'Street Address' column
data = data.withColumn('Street Address', remove_extra_spaces(col('Street Address')))


# In[33]:


# Calculate the percentage of null values in each columnafter data cleaning
num_rows = data.count()
null_percentages = [(count(when(col(c).isNull(), c)) / num_rows * 100).alias(c) for c in data.columns]

# Create a DataFrame with the null percentages
null_percentage_df = data.agg(*null_percentages)

# Show the null percentages
# null_percentage_df.show()


# In[34]:


# Calculate the duration

min_date = data.select(min('Date Occurred')).first()[0]
max_date = data.select(max('Date Occurred')).first()[0]
duration = (max_date - min_date).days

print("There are {} crimes committed over {} days. On average, there are {} crimes each day.".format(data.count(), duration, int(data.count() / duration)))


# In[35]:


# Calculate the delay
data = data.withColumn('Delay', datediff(col('Date Reported'), col('Date Occurred')))
print("Calculated the delay!")


# In[36]:


# Define bins and labels
bins = [-1, 0, 1, 2, 7, 30, 365, float('inf')]
labels = ["Same day", "1 day", "2 days", '3-7 days', '8-30 days', '1 month-1 year', 'More than 1 year']

# Create 'Delay Category' column
data = data.withColumn('Delay Category',
    when((col('Delay') >= bins[0]) & (col('Delay') < bins[1]), labels[0])
    .when((col('Delay') >= bins[1]) & (col('Delay') < bins[2]), labels[1])
    .when((col('Delay') >= bins[2]) & (col('Delay') < bins[3]), labels[2])
    .when((col('Delay') >= bins[3]) & (col('Delay') < bins[4]), labels[3])
    .when((col('Delay') >= bins[4]) & (col('Delay') < bins[5]), labels[4])
    .when((col('Delay') >= bins[5]) & (col('Delay') < bins[6]), labels[5])
    .otherwise(labels[6]))

print("Created the delay category!")


# In[37]:


# Create 'Week Period' column
data = data.withColumn('Week Period', date_trunc('week', col('Date Occurred')))

# Create 'Month Period' column
data = data.withColumn('Month Period', date_trunc('month', col('Date Occurred')))


# In[38]:


# Create 'Time Slot' column
data = data.withColumn(
    'Time Slot',
    when((col('Time Occurred').cast('long') >= float('-inf')) & (col('Time Occurred').cast('long') < 1.59 * 3600), '0-2')
    .when((col('Time Occurred').cast('long') >= 1.59 * 3600) & (col('Time Occurred').cast('long') < 3.59 * 3600), '2-4')
    .when((col('Time Occurred').cast('long') >= 3.59 * 3600) & (col('Time Occurred').cast('long') < 5.59 * 3600), '4-6')
    .when((col('Time Occurred').cast('long') >= 5.59 * 3600) & (col('Time Occurred').cast('long') < 7.59 * 3600), '6-8')
    .when((col('Time Occurred').cast('long') >= 7.59 * 3600) & (col('Time Occurred').cast('long') < 9.59 * 3600), '8-10')
    .when((col('Time Occurred').cast('long') >= 9.59 * 3600) & (col('Time Occurred').cast('long') < 11.59 * 3600), '10-12')
    .when((col('Time Occurred').cast('long') >= 11.59 * 3600) & (col('Time Occurred').cast('long') < 13.59 * 3600), '12-14')
    .when((col('Time Occurred').cast('long') >= 13.59 * 3600) & (col('Time Occurred').cast('long') < 15.59 * 3600), '14-16')
    .when((col('Time Occurred').cast('long') >= 15.59 * 3600) & (col('Time Occurred').cast('long') < 17.59 * 3600), '16-18')
    .when((col('Time Occurred').cast('long') >= 17.59 * 3600) & (col('Time Occurred').cast('long') < 19.59 * 3600), '18-20')
    .when((col('Time Occurred').cast('long') >= 19.59 * 3600) & (col('Time Occurred').cast('long') < 21.59 * 3600), '20-22')
    .when((col('Time Occurred').cast('long') >= 21.59 * 3600) & (col('Time Occurred').cast('long') < float('inf')), '22-24')
    .otherwise(None)
)

print("Created 'Time Slot' column")

# In[42]:





# In[39]:


# Save DataFrame to CSV
# data.write.mode("overwrite").csv(r"processed-los-angeles-data.csv", header=True)
# data.toPandas().to_csv('mycsv.csv')

data_df = data.toPandas()
credentials = service_account.Credentials.from_service_account_file("/creds/lacrimedataanalysis.json")
pandas_gbq.context.credentials = credentials
pandas_gbq.to_gbq(data_df, destination_table='los_angeles.la_crime_data_testing',
           project_id='lacrimedataanalysis',
           if_exists='replace')


print("##########################")
print("           END            ")
print("##########################")


# In[ ]:





# In[ ]:





# ## Time Series Modelling

# In[ ]:


# time_series_data = data.select('Date Occurred')

# time_series_data = data.groupBy('Date Occurred').count().withColumnRenamed('count', 'Crime Count')

# Show the resulting time series data
# time_series_data.show(5)


# In[ ]:


# time_series_data = time_series_data.withColumn('Month', month('Date Occurred'))
# time_series_data = time_series_data.withColumn('Year', year('Date Occurred'))

# Show the resulting time series data
# time_series_data.show(10)


# In[ ]:


# Group by 'Year' and 'Month' and pivot the data
# crosstab_data = time_series_data.groupBy('Year').pivot('Month').agg(F.sum('Crime Count'))

# Show the resulting crosstab data
# crosstab_data.show(truncate=False)


# In[ ]:


# data = pd.read_csv('final_los_angeles_crime_data.csv')
# data.head()


# # In[ ]:


# time_series_data = data[['Date Occured']][:]
# time_series_data = time_series_data.groupby(['Date Occured']).size().reset_index(name='Crime Count')
# # Convert the string to a datetime object
# time_series_data['Date Occured'] = pd.to_datetime(time_series_data['Date Occured'])
# time_series_data.head()


# # In[ ]:


# # Function to get month from a date
# def Function_get_month(inpDate):
#     return(inpDate.month)

# # Function to get Year from a date
# def Function_get_year(inpDate):
#     return(inpDate.year)

# # Creating new columns
# time_series_data['Month']=time_series_data['Date Occured'].apply(Function_get_month)
# time_series_data['Year']=time_series_data['Date Occured'].apply(Function_get_year)

# time_series_data.head()
# time_series_data.set_index('Date Occured', inplace=True)
# time_series_data.head()


# # In[ ]:


# # Aggregating the sales quantity for each month for all categories
# pd.crosstab(columns=time_series_data['Month'],
#             index=time_series_data['Year'],
#             values=time_series_data['Crime Count'],
#             aggfunc='sum')


# # In[ ]:


# CrimeCount=pd.crosstab(columns=time_series_data['Year'],
#             index=time_series_data['Month'],
#             values=time_series_data['Crime Count'],
#             aggfunc='sum').melt()['value']

# MonthNames=['Jan','Feb','Mar','Apr','May', 'Jun', 'Jul', 'Aug', 'Sep','Oct','Nov','Dec']*4

# # Plotting the sales
# # get_ipython().run_line_magic('matplotlib', 'inline')
# # CrimeCount.plot(kind='line', figsize=(16,5), title='Total Crime Count per Month')
# # # Setting the x-axis labels
# # plotLabels=plt.xticks(np.arange(0,48,1),MonthNames, rotation=30)


# # In[ ]:


# series = CrimeCount.values
# result = seasonal_decompose(series, model='additive', period=12)
# # result.plot()
# CurrentFig=plt.gcf()
# CurrentFig.set_size_inches(11,8)
# plt.show()


# # In[ ]:


# CrimeCount = CrimeCount[:len(CrimeCount)-1]


# # In[ ]:


# CrimeCount = (CrimeCount/3800000)*100000


# # In[ ]:


# # Train the model on the full dataset 
# SarimaxModel = model = SARIMAX(CrimeCount,  
#                         order = (5, 1, 10),  
#                         seasonal_order = (6, 0, 0, 12))
# CrimeModel = SarimaxModel.fit()
  
# # Forecast for the next 6 months
# forecast = CrimeModel.predict(start = 0,
#                           end = (len(CrimeCount)+7)-1,
#                           typ = 'levels').rename('Forecast')
# print("Next Six Month Forecast:",forecast[-7:])

# # Plot the forecast values
# # CrimeCount.plot(figsize = (10, 15), legend = True, title='Time Series Crime Count Forecasts')
# # forecast.plot(legend = True, figsize=(18,5))

# # Measuring the accuracy of the model
# MAPE=np.mean(abs(CrimeCount-forecast)/CrimeCount)*100
# print('#### Accuracy of model:', round(100-MAPE,2), '####')

# # Printing month names in X-Axis
# # Printing month names in X-Axis
# # MonthNames2=MonthNames+MonthNames[0:6]
# # plotLabels=plt.xticks(np.arange(0,54,1),MonthNames2, rotation=45)


# # In[ ]:





# # In[ ]:





# # In[ ]:


# # Convert Pandas DataFrame to PySpark DataFrame
# gbq_data_pyspark = spark.createDataFrame(data)

# # Rename columns in PySpark DataFrame
# for column in gbq_data_pyspark.columns:
#     new_column = column.replace(" ", "_").replace("-", "_")
#     gbq_data_pyspark = gbq_data_pyspark.withColumnRenamed(column, new_column)

# # Show the columns in the PySpark DataFrame
# print(gbq_data_pyspark.columns)


# # In[ ]:


# # Define your BigQuery destination table
# destination_table = "los_angeles.la_crime_data"

# # Write the PySpark DataFrame to BigQuery
# gbq_data_pyspark.write \
#     .format("bigquery") \
#     .option("table", destination_table) \
#     .option("project", "lacrimedataanalysis") \
#     .mode("overwrite") \
#     .save()


# # In[ ]:





# # In[ ]:





# # In[44]:


# df_temp = pd.read.csv("la_crime_data.csv", header=True, inferSchema=True)
# df_temp.head(10000).to_csv("la_crime_data_10000.csv")


# # In[ ]:




