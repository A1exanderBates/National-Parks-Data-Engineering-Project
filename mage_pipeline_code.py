# data loader

import requests
import json
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    record_limit = 500

    API_KEY = env_var
    url = f"https://developer.nps.gov/api/v1/parks?limit={record_limit}"


    params = {
        'api_key': API_KEY,
    }
    response = requests.get(url, params=params)

    # Check if the response was successful (HTTP status code 200)
    if response.status_code == 200:
        # Print the response data
        print('Success')
    else:
        # Print the error status code and message
        print(f"Error: {response.status_code} - {response.text}")

    data = response.json()
    json_str = json.dumps(data, indent=4)
    json_object = json.loads(json_str)
    data = json_object["data"]

    return data



# data transformer
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    rows = []
    for park in data:
        park_id = park['id']
        park_url = park['url']
        park_name = park['fullName']
        short_name = park['name']
        park_code = park['parkCode']
        park_state = park['states']
        latitude = park['latitude']
        longitude = park['longitude']
        directions_info = park['directionsInfo']
        directions_url = park['directionsUrl']
        weather_info = park['weatherInfo']

        rows.append([park_id, park_url, park_name, short_name, park_code, park_state,
                    latitude, longitude, directions_info, directions_url, weather_info])

    column_headers = ['park_id', 'url', 'full_name', 'short_name', 'park_code', 'states', 'latitude', 'longitude', 'directions_info', 'directions_url', 'weather_info']
    basic_park_info_df = pd.DataFrame(rows, columns=column_headers)


    rows = []
    for park in data:
        park_id = park['id']
        for activity in park['activities']:
            activity_id = activity['id']
            activity_name = activity['name']

            rows.append([park_id, activity_id, activity_name])

    column_headers = ['park_id', 'activities_id', 'activities_name']
    activities_df = pd.DataFrame(rows, columns=column_headers)

    # create an empty list to hold the rows
    rows = []

    # iterate through each park in data
    for park in data:
        # get park ID
        park_id = park['id']
        # iterate through each address for the park
        for address in park['addresses']:
            # create a new row with data from the address and park ID
            row = [
                park_id,
                address['postalCode'],
                address['city'],
                address['stateCode'],
                address['line1'],
                address['line2'],
                address['line3'],
                address['type']
            ]
            # append the row to the list of rows
            rows.append(row)

    # create a dataframe from the rows and define the column names
    address_df = pd.DataFrame(rows, columns=[
        'park_id',
        'postal_code',
        'city',
        'state_code',
        'address_line1',
        'address_line2',
        'address_line3',
        'address_type'
    ])

    rows = []
    for park in data:
        for hours in park['operatingHours']:
            row = [park['id'],
                hours['description'],
                hours['standardHours']['monday'],
                hours['standardHours']['tuesday'],
                hours['standardHours']['wednesday'],
                hours['standardHours']['thursday'],
                hours['standardHours']['friday'],
                hours['standardHours']['saturday'],
                hours['standardHours']['sunday']]
            rows.append(row)

    operating_hours_df = pd.DataFrame(rows, columns=['park_id',
                                                    'operating_hours_description',
                                                    'standard_hours_monday',
                                                    'standard_hours_tuesday',
                                                    'standard_hours_wednesday',
                                                    'standard_hours_thursday',
                                                    'standard_hours_friday',
                                                    'standard_hours_saturday',
                                                    'standard_hours_sunday'])


    rows = []
    for park in data:
        park_id = park['id']
        contacts = park['contacts']
        phone_numbers = contacts['phoneNumbers']
        email_addresses = contacts['emailAddresses']

        for phone_number in phone_numbers:
            row = {'park_id': park_id,
                'phone_number': phone_number['phoneNumber'],
                'phone_description': phone_number['description'],
                'phone_extension': phone_number['extension'],
                'phone_type': phone_number['type'],
                'email_dddress': email_addresses[0]['emailAddress']}
            rows.append(row)

    contacts_df = pd.DataFrame(rows)

    # Entrance fees

    entrance_fees_df = pd.DataFrame(
        [(park['id'],
        fee['cost'],
        fee['description'],
        fee['title']) for park in data for fee in park['entranceFees']],
        columns=['park_id', 'entrance_fees_cost',
                'entrance_fees_description', 'entrance_fees_title']
    )

    entrance_pass_df = pd.DataFrame(
        [(park['id'],
        fee['cost'],
        fee['description'],
        fee['title']) for park in data for fee in park['entrancePasses']],
        columns=['park_id', 'entrance_pass_cost',
                'entrance_pass_description', 'entrance_pass_title']
    )


    df1 = basic_park_info_df.merge(activities_df, on='park_id', how='left')
    df2 = df1.merge(address_df, on='park_id', how='left')
    df3 = df2.merge(contacts_df, on='park_id', how='left')
    df4 = df3.merge(entrance_fees_df, on='park_id', how='left')
    df5 = df4.merge(entrance_pass_df, on='park_id', how='left')
    df6 = df5.merge(operating_hours_df, on='park_id', how='left')
    
    df6['entrance_fees_description'] = df6['entrance_fees_description'].str.replace('\n', '\\n')
    df6['entrance_pass_description'] = df6['entrance_pass_description'].str.replace('\n', '\\n')

    final_wide_table = df6

    return final_wide_table





#data exporter
from mage_ai.data_preparation.repo_manager import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.s3 import S3
from pandas import DataFrame
from os import path
import boto3
import csv

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_exporter
def export_data_to_s3(df: DataFrame, **kwargs) -> None:
    # Specify your S3 bucket information
    bucket_name = 'nationalparks3bucket'
    object_key = 'final_wide_table_nps.csv'

    # Create a Boto3 S3 client using the default credentials from the IAM role assigned to the EC2 instance
    s3_client = boto3.client('s3')

    # Convert DataFrame to CSV
    csv_data = df.to_csv(index=False, sep='|', quoting=csv.QUOTE_MINIMAL)

    # Upload the CSV data to S3
    try:
        s3_client.put_object(Body=csv_data, Bucket=bucket_name, Key=object_key)
        print("Data export to S3 was successful!")
    except Exception as e:
        print(f"Data export to S3 failed. Error: {str(e)}")


   
