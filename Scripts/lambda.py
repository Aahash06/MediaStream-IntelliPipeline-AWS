import json
import boto3
import requests
from urllib.parse import urljoin
 
def lambda_handler(event, context):
    mwaa_env_name = "Aahash-iceberg"
    dag_name = "trigger_glue_merge_dag"  
 
    # Step 1: Create MWAA client
    client = boto3.client('mwaa')
 
    # Step 2: Get CLI token & webserver hostname
    resp = client.create_cli_token(Name=mwaa_env_name)
    token = resp['CliToken']
    webserver_hostname = resp['WebServerHostname']
 
    # Step 3: Trigger DAG using MWAA CLI
    dag_trigger_cmd = f"dags trigger {dag_name}"
 
    url = f"https://{webserver_hostname}/aws_mwaa/cli"
    headers = {
        'Authorization': f"Bearer {token}",
        'Content-Type': 'text/plain'
    }
 
    mwaa_response = requests.post(
        url,
        headers=headers,
        data=dag_trigger_cmd.encode('utf-8')
    )
 
    print("Lambda triggered with the following event:")
    print(json.dumps(event, indent=4))
    print("DAG trigger response:")
    print(mwaa_response.text)
 
    return {
        'statusCode': 200,
        'body': 'Triggered MWAA DAG from Lambda'
    }
