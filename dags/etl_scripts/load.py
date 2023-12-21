import pandas as pd
from google.oauth2 import service_account
import pandas_gbq


def load_data(**context):
    data_df = pd.read_csv(f"/tmp/data_df_{context['ds']}")
    credentials = service_account.Credentials.from_service_account_file(
        "/creds/lacrimedataanalysis.json"
    )
    pandas_gbq.context.credentials = credentials
    pandas_gbq.to_gbq(
        data_df,
        destination_table="los_angeles.la_crime_data_testing",
        project_id="lacrimedataanalysis",
        if_exists="replace",
    )
    pass
