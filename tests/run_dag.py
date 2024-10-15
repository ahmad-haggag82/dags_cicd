import boto3
import json
import requests
import base64
import argparse, sys
import time


def resolve_args():
    default_env = 'sfly-aws-dwh-lifetouch-dev-lifetouch-mwaa-v1'
    parser = argparse.ArgumentParser()
    parser.add_argument("--mwaa_env", help="MWAA Environment",
                        default=default_env)
    parser.add_argument("--dag_name", help="Dag Name")
    _args = parser.parse_args()
    return _args


def get_cli_token(mwaa_env_name):
    mwaa_client = boto3.client('mwaa', 'us-east-1')
    mwaa_cli_token = mwaa_client.create_cli_token(Name=mwaa_env_name)
    _auth_token = 'Bearer ' + mwaa_cli_token['CliToken']
    _webserver_hostname = (f"https://"
                           f"{mwaa_cli_token['WebServerHostname']}/aws_mwaa/cli")
    return _auth_token, _webserver_hostname


def create_dag_request(webserver_hostname, auth_token, dag_cli):
    mwaa_response = requests.post(
        webserver_hostname,
        headers={
            'Authorization': auth_token,
            'Content-Type': 'text/plain'
        },
        data=dag_cli
    )
    return mwaa_response




def run_dag(webserver_hostname, auth_token, dag_name):
    run_dag_cli = f"dags trigger {dag_name}"
    _response = create_dag_request(
        webserver_hostname=webserver_hostname,
        auth_token=auth_token,
        dag_cli=run_dag_cli)
    _run_response = base64.b64decode(_response.json()['stdout']).decode('utf8')
    print(_run_response)
    return _run_response
    

def get_execution_date(webserver_hostname, auth_token, dag_name):
    execution_date_cli = f"dags list-runs -d {dag_name} --state running -o json"
    mwaa_response = create_dag_request(
           webserver_hostname=webserver_hostname,
            auth_token=auth_token,
            dag_cli=execution_date_cli)

    running_list = base64.b64decode(mwaa_response.json()['stdout']).decode('utf8')
    _execution_date = json.loads(running_list)[0]["execution_date"]
    return _execution_date


def check_dag_status(env_name, webserver_hostname, auth_token, dag_name):
    execution_date = get_execution_date(webserver_hostname, auth_token, dag_name)
    print(f"execution date = {execution_date}")
    dag_status_cli = f"dags state {dag_name} {execution_date}"
    _dag_status = 'Running'
    while _dag_status == 'Running':
        time.sleep(60)
        mwaa_auth_token, mwaa_webserver_hostname = get_cli_token(env_name)
        dag_status_response = create_dag_request(
            webserver_hostname=mwaa_webserver_hostname,
            auth_token=mwaa_auth_token,
            dag_cli=dag_status_cli)

        _dag_status = base64.b64decode(dag_status_response.json()['stdout']).decode('utf8')
        print(f"dag status = {_dag_status}")
    
    return _dag_status



def main():
    args = resolve_args()
    mwaa_env = args.mwaa_env
    dag_name = args.dag_name
    mwaa_auth_token, mwaa_webserver_hostname = get_cli_token(mwaa_env)
    run_dag(mwaa_webserver_hostname, mwaa_auth_token, dag_name)
    dag_status = check_dag_status(mwaa_env, mwaa_webserver_hostname, mwaa_auth_token, dag_name)
    if dag_status == 'failed':
        raise Exception(f"Dag {dag_name} failed, please verify it")




if __name__ == "__main__":
    main()
