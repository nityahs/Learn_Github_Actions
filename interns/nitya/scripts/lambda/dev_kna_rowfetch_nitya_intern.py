import json
import psycopg2
import os
from psycopg2.extras import RealDictCursor
import boto3
import logging
s3 = boto3.client("s3")
logger=logging.getLogger()
secman=boto3.client("secretsmanager")
secret_name=os.environ['SecretName']
def dataconnection(secret_name):
    try:
        response = secman.get_secret_value(SecretId=secret_name)
        secrets = json.loads(response['SecretString'])
        database = secrets['dbname']
        user=secrets['username']
        host=secrets['host']
        password=secrets['password']
        port=os.environ['Port']
        conn = psycopg2.connect(
        user=user,
        password=password,
        host=host,
        database=database,
        port=port,
        cursor_factory=RealDictCursor)
        logger.info("Database connected")
        curr = conn.cursor()
        return conn, curr
    except Exception as e:
        print(e)
        logger.error(f"Couldn't connect to database")
def lambda_handler(event, context):
    try:
        print(event)
        execId=event['ExecutionId']
        conn,curr=dataconnection(secret_name)
        curr.execute("""select filename,filekey,bucketname,lastmodified from public.intrn_kna_nitya_lambdards where filestatus='Registered';""")
        rdsdata=curr.fetchall()
        print(rdsdata)
        logger.info("Data Fetched")
        curr.execute("""update public.intrn_kna_nitya_lambdards set filestatus='Running', executionid=(%s) where filestatus ='Registered';""",(execId,))
        conn.commit()
        curr.close()
        conn.close()
        
        return rdsdata
    except Exception as e:
        curr.execute("""update public.intrn_kna_nitya_lambdards set filestatus='Failed', executionid=(%s) where filestatus ='Running';""",(execId,))
        conn.commit()
        curr.close()
        conn.close()
        return{
            "ProcessName":"FetchRow",
            "Error":e
        }

