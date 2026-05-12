import boto3
import pandas as pd
import psycopg2
import logging
from io import BytesIO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# config
BUCKET = "koffee-store"
SILVER_PREFIX = "silver/"

# redshift config
RS_HOST = "my-server.375170594858.ap-south-1.redshift-serverless.amazonaws.com"
RS_PORT = 5439
RS_DB = "dev"
RS_USER = "admin"
# RS_PASSWORD = "RYDRIhsud502%)"

import boto3
import psycopg2

def get_redshift_connection():
    try:
        # get temporary credentials from IAM
        client = boto3.client('redshift-serverless', region_name='ap-south-1')
        
        credentials = client.get_credentials(
            dbName='dev',
            workgroupName='your-workgroup-name'    # just the name, not full endpoint
        )
        
        conn = psycopg2.connect(
            host=RS_HOST,
            port=RS_PORT,
            database=RS_DB,
            user=credentials['dbUser'],
            password=credentials['dbPassword']
        )
        logger.info("Redshift connection established")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Redshift: {e}")
        raise

def get_max_datetime_from_redshift(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(sale_date) FROM orders;")
        result = cursor.fetchone()[0]
        cursor.close()
        logger.info(f"Max datetime in Redshift: {result}")
        return result
    except Exception as e:
        logger.error(f"Failed to get max datetime: {e}")
        raise

def get_latest_silver_file(bucket, prefix):
    try:
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = response['Contents']
        latest = sorted(files, key=lambda x: x['LastModified'], reverse=True)[0]
        logger.info(f"Latest silver file: {latest['Key']}")
        return latest['Key']
    except Exception as e:
        logger.error(f"Failed to get latest silver file: {e}")
        raise

def read_parquet_from_s3(bucket, key):
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=key)
        buffer = BytesIO(response['Body'].read())
        df = pd.read_parquet(buffer)
        logger.info(f"Read {len(df)} rows from Silver")
        return df
    except Exception as e:
        logger.error(f"Failed to read parquet: {e}")
        raise

def filter_new_rows(df, max_datetime):
    try:
        if max_date is None:
            # redshift is empty, load everything
            logger.info("Redshift is empty. Loading all rows.")
            return df
        
        df['sale_date'] = pd.to_datetime(df['sale_date'])
        new_rows = df[df['sale_date'] > pd.Timestamp(max_datetime)]
        logger.info(f"New rows to load: {len(new_rows)}")
        return new_rows
    except Exception as e:
        logger.error(f"Failed to filter new rows: {e}")
        raise

def load_to_redshift(df, conn):
    try:
        if len(df) == 0:
            logger.info("No new rows to load. Exiting.")
            return
        
        cursor = conn.cursor()
        
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO orders 
                (sale_date, cash_type, card, amount, coffee_name)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                row['sale_date'],
                row['cash_type'],
                row['card'],
                row['amount'],
                row['coffee_name']
            ))
        
        conn.commit()
        cursor.close()
        logger.info(f"Successfully loaded {len(df)} rows into Redshift")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to load to Redshift: {e}")
        raise

def main():
    conn = get_redshift_connection()
    
    # step 1 — get max datetime already in redshift
    max_datetime = get_max_datetime_from_redshift(conn)
    
    # step 2 — get latest silver file
    silver_key = get_latest_silver_file(BUCKET, SILVER_PREFIX)
    
    # step 3 — read silver parquet
    df = read_parquet_from_s3(BUCKET, silver_key)
    
    # step 4 — filter only new rows
    df = filter_new_rows(df, max_datetime)
    
    # step 5 — load to redshift
    load_to_redshift(df, conn)
    
    conn.close()
    logger.info("Pipeline complete")

main()