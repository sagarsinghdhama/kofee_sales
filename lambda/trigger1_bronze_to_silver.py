import boto3

def lambda_handler(event, context):
    glue = boto3.client('glue')
    
    glue.start_job_run(JobName='coffee_orders_script')
    
    print("Glue job triggered successfully")
    return {'statusCode': 200}