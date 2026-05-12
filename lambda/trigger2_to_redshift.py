import boto3
import time

def lambda_handler(event, context):
    # get the exact file that just landed in Silver
    key = event['Records'][0]['s3']['object']['key']
    
    client = boto3.client('redshift-data', region_name='ap-south-1')
    
    response = client.execute_statement(
        WorkgroupName='my-server',
        Database='dev',
        Sql=f"""
            COPY coffee_sales
            FROM 's3://koffee-store/{key}'
            IAM_ROLE 'arn:aws:iam::375170594858:role/service-role/AmazonRedshift-CommandsAccessRole-20260506T010559'
            FORMAT AS PARQUET;
        """
    )
    
    # wait for query to complete
    statement_id = response['Id']
    
    while True:
        status = client.describe_statement(Id=statement_id)['Status']
        if status == 'FINISHED':
            print("COPY completed successfully")
            break
        elif status == 'FAILED':
            error = client.describe_statement(Id=statement_id)['Error']
            raise Exception(f"COPY failed: {error}")
        time.sleep(3)
    
    return {'statusCode': 200}