import requests
import pandas as pd
import datetime
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta

API_URL = "https://data.lacity.org/resource/2nrs-mtv8.json"

API_TO_DB_COLUMNS = {
' C0':'c0',
 'Dr No':'dr_no',
 'Date Reported': 'date_reported',
 'Date Occurred': 'date_occurred',
 'Time Occurred': 'time_occurred',
 'Area Code': 'area_code',
 'Area Name': 'area_name',
 'Rpt Dist No':'rpt_dist_no',
 'Part 1-2':'part_1_2',
 'Crime Code':'crime_code',
 'Crime Description':'crime_description',
 'Modus Operandi':'modus_operandi',
 'Victim Age': 'victim_age',
 'Victim Gender': 'victim_gender',
 'Victim Descent': 'victim_descent',
 'Crime Location': 'crime_location',
 'Case Status': 'case_status',
 'Street Address': 'street_address',
 'Latitude': 'latitude',
 'Longitude': 'longitude',
 'Weapon Description': 'weapon_description',
 'Cross Street': 'cross_street',
 'Age Group': 'age_group',
 'Delay': 'delay_time',
 'Delay Category': 'delay_category',
 'Week Period': 'week_period',
 'Month Period': 'month_period',
 'Time Slot': 'time_slot'
}

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
    return pd.DataFrame(data)


def load_data(start_date, end_date, batch_size=50000, debug=True):
    # Get total count of columns to load
    records_count = get_count(start_date, end_date)
    print(f"Fetching {records_count} records...")

    num_batches = int(records_count / batch_size) + 1

    # Fetch data in batches
    all_data = []
    for batch_num in range(num_batches):
        batch_data = get_data(start_date, end_date, batch_size, batch_num)
        all_data.append(batch_data)

    return pd.concat(all_data).drop_duplicates()

def extract_data(connection_string, **context):
    start_date = (datetime.datetime.strptime(context["ds"], "%Y-%m-%d") - relativedelta(months=1)).strftime("%Y-%m-%d")
    end_date = context["ds"]

    df = load_data(start_date, end_date)
    # df = df.rename(columns=API_TO_DB_COLUMNS)

    conn = create_engine(connection_string)

    print("Writing to database")
    df.to_sql("lacrimeanalysis", conn, if_exists='append', index=False)

