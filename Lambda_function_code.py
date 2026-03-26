************Lambda function code***********
import json
import pandas as pd
import requests
import boto3
import consonants as con
from io import StringIO
sns = boto3.client("sns", region_name="eu-north-1")
ssm = boto3.client("ssm", region_name="eu-north-1")
# github_api_url = ssm.get_parameter(Name=con.urlapi, WithDecryption=True)["Parameter"]["Value"]

def send_sns_success():
    success_sns_arn = ssm.get_parameter(Name=con.SUCCESSNOTIFICATIONARN, WithDecryption=True)["Parameter"]["Value"]
    component_name = con.COMPONENT_NAME
    env = ssm.get_parameter(Name=con.ENVIRONMENT, WithDecryption=True)['Parameter']['Value']
    success_msg = con.SUCCESS_MSG
    sns_message = (f"{component_name} :  {success_msg}")
    print(sns_message, 'text')
    succ_response = sns.publish(TargetArn=success_sns_arn,Message=json.dumps({'default': json.dumps(sns_message)}),
        Subject= env + " : " + component_name,MessageStructure="json")
    return succ_response
        
def send_error_sns(msg):
    error_sns_arn = ssm.get_parameter(Name=con.ERRORNOTIFICATIONARN)["Parameter"]["Value"]
    env = ssm.get_parameter(Name=con.ENVIRONMENT, WithDecryption=True)['Parameter']['Value']
    error_message=con.ERROR_MSG+msg
    component_name = con.COMPONENT_NAME
    sns_message = (f"{component_name} : {error_message}")
    err_response = sns.publish(TargetArn=error_sns_arn,Message=json.dumps({'default': json.dumps(sns_message)}),    Subject=env + " : " + component_name,
        MessageStructure="json")
    return err_response


def lambda_handler(event, context):
    try:
        github_api_url = ssm.get_parameter(Name=con.urlapi, WithDecryption=True)["Parameter"]["Value"]
        url1 = 'https://api.github.com/repos/squareshift/stock_analysis/content/'
        # response = requests.get(github_api_url)
        response = requests.get(url1)
        response.raise_for_status()  # Raise an exception for HTTP errors
        files = response.json()
        csv_files = [file['download_url'] for file in files if file['name'].endswith('.csv')]
        csv_file = csv_files.pop()
        d = pd.read_csv(csv_file)
        
        dataframes=[]
        file_names=[]
        for url in csv_files:
            file_name = url.split("/")[-1].replace(".csv", "")
            df = pd.read_csv(url)
            df['Symbol'] = file_name
            dataframes.append(df)
            file_names.append(file_name)

        combined_df = pd.concat(dataframes, ignore_index=True)
        o_df = pd.merge(combined_df,d,on='Symbol',how='left')
        
        result = o_df.groupby("Sector").agg({'open':'mean','close':'mean','high':'max','low':'min','volume':'mean'}).reset_index()
        
        o_df["timestamp"] = pd.to_datetime(o_df["timestamp"])
        filtered_df = o_df[(o_df['timestamp'] >= "2021-01-01") & (o_df['timestamp'] <="2021-05-26")]
        
        list_sector = ["TECHNOLOGY","FINANCE"]
        result_time = filtered_df.groupby("Sector").agg({'open':'mean','close':'mean','high':'max','low':'min','volume':'mean'}).reset_index()
        result_time = result_time.rename(columns={'open': 'aggregate_open',
                                                  'close': 'aggregate_close',
                                                  'high': 'aggregate_high',
                                                  'low': 'aggregate_low',
                                                  'volume': 'aggregate_volume'})
        result_time = result_time[result_time["Sector"].isin(list_sector)].reset_index(drop=True)
        
        csv_buffer = StringIO()
        print(result_time)
        result_time.to_csv(csv_buffer, index=False)
        
        # Upload CSV to S3
        s3 = boto3.client('s3')
        bucket_name = 'deviapioutput'
        key = 'deviapioutput/data/stock.csv'
        s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=key)
        send_sns_success()
        
        return {
            'statusCode': 200,
            'body': 'Data uploaded to S3 successfully!'
        }
    except Exception as e:
        print(e)
        msg=str(e)
        send_error_sns(msg)
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
            
        }


______________________________________________________________________________________________________
************Consonants.py*********************

import datetime
def get_datetime():
    dt1 = datetime.datetime.now()
    print(dt1)
    return dt1.strftime("%d %B, %Y")
monthstr = get_datetime()
print(monthstr)
urlapi= '/data/repourl'
ERRORNOTIFICATIONARN = '/data/errorarn'
SUCCESSNOTIFICATIONARN='/data/successarn'
COMPONENT_NAME = 'DL_DATA_REPO_EXTRACT'
# ERROR_MSG = f'NEED ATTENTION ****API ERROR /KEY EXPIRED ** ON {monthstr} ******'
ERROR_MSG = f'NEED ATTENTION ****URL GOT CHANGED  ** ON {monthstr} ******'
SUCCESS_MSG = f'SUCCESSFULLY EXTRACTED  FILES FOR {monthstr}*'
SUCCESS_DESCRIPTION='SUCCESS'
ENVIRONMENT = '/data/env'
_________________________________________________________________________________________________________

*************old lambda function code*****************
import json
import pandas
import requests
import boto3
import consonants as con
sns = boto3.client("sns", region_name="eu-north-1")
ssm = boto3.client("ssm", region_name="eu-north-1")
github_api_url = ssm.get_parameter(Name=con.urlapi, WithDecryption=True)["Parameter"]["Value"]
def send_sns_success():
    success_sns_arn = ssm.get_parameter(Name=con.SUCCESSNOTIFICATIONARN, WithDecryption=True)["Parameter"]["Value"]
    component_name = con.COMPONENT_NAME
    env = ssm.get_parameter(Name=con.ENVIRONMENT, WithDecryption=True)['Parameter']['Value']
    success_msg = con.SUCCESS_MSG
    sns_message = (f"{component_name} :  {success_msg}")
    print(sns_message, 'text')
    succ_response = sns.publish(TargetArn=success_sns_arn,Message=json.dumps({'default': json.dumps(sns_message)}),
        Subject= env + " : " + component_name,MessageStructure="json")
    return succ_response
        
def send_error_sns():
    error_sns_arn = ssm.get_parameter(Name=con.ERRORNOTIFICATIONARN)["Parameter"]["Value"]
    env = ssm.get_parameter(Name=con.ENVIRONMENT, WithDecryption=True)['Parameter']['Value']
    error_message=con.ERROR_MSG
    component_name = con.COMPONENT_NAME
    sns_message = (f"{component_name} : {error_message}")
    err_response = sns.publish(TargetArn=error_sns_arn,Message=json.dumps({'default': json.dumps(sns_message)}),    Subject=env + " : " + component_name,
        MessageStructure="json")
    return err_response


def lambda_handler(event, context):
    try:
        github_api_url = "YOUR_GITHUB_API_URL"
        response = requests.get(github_api_url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        files = response.json()
        csv_files = [file['download_url'] for file in files if file['name'].endswith('.csv')]
        csv_file = csv_files.pop()
        
        d = pd.read_csv(csv_file)
        
        dataframes=[]
        file_names=[]
        for url in csv_files:
            file_name = url.split("/")[-1].replace(".csv", "")
            df = pd.read_csv(url)
            df['Symbol'] = file_name
            dataframes.append(df)
            file_names.append(file_name)

        combined_df = pd.concat(dataframes, ignore_index=True)
        o_df = pd.merge(combined_df,d,on='Symbol',how='left')
        
        result = o_df.groupby("Sector").agg({'open':'mean','close':'mean','high':'max','low':'min','volume':'mean'}).reset_index()
        
        o_df["timestamp"] = pd.to_datetime(o_df["timestamp"])
        filtered_df = o_df[(o_df['timestamp'] >= "2021-01-01") & (o_df['timestamp'] <="2021-05-26")]
        
        list_sector = ["TECHNOLOGY","FINANCE"]
        result_time = filtered_df.groupby("Sector").agg({'open':'mean','close':'mean','high':'max','low':'min','volume':'mean'}).reset_index()
        result_time = result_time.rename(columns={'open': 'aggregate_open',
                                                  'close': 'aggregate_close',
                                                  'high': 'aggregate_high',
                                                  'low': 'aggregate_low',
                                                  'volume': 'aggregate_volume'})
        result_time = result_time[result_time["Sector"].isin(list_sector)].reset_index(drop=True)
        
        csv_buffer = StringIO()
        result_time.to_csv(csv_buffer, index=False)
        
        # Upload CSV to S3
        s3 = boto3.client('s3')
        bucket_name = 'deviapioutput'
        key = 'deviapioutput/data/stock.csv'
        s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=key)
        send_sns_success()
        
        return {
            'statusCode': 200,
            'body': 'Data uploaded to S3 successfully!'
        }
    except Exception as e:
        print(e)
        send_error_sns()
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
            
        }



