from datetime import datetime
import pandas as pd
import boto3
from io import StringIO

from numpy.f2py.crackfortran import publicpattern


def handle_insert(record):
    print("handling insert: ", record)
    dictionary = {}
    for  key, value in record['dynamodb']['NewImage'].items():
        for dt, col in value.items():
            dictionary.update({key: col})

    dff = pd.DataFrame([dictionary])
    return dff


def lambda_handler(event, context):
    global table, dff
    print(event)
    df = pd.DataFrame()

    for record in event['Records']:
        table = record['eventSourceARN'].split("/")[1]

        if record['eventName'] == "INSERT":
            dff = handle_insert(record)
        df = dff

    if not df.empty:
        all_columns = list(df)
        df[all_columns] = df[all_columns].astype(str)

        path = table + "_" + str(datetime.now()) + ".csv"
        print(event)

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        s3 = boto3.client('s3')
        bucket = "itvorx"
        key = "snowflake/" + table + "_" + str(datetime.now()) + ".csv"
        print(key)

        s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue(), )

    print('Successfully processed %s records.' % str(len(event['Records'])))
