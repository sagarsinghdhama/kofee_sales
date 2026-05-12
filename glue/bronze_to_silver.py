import boto3
import pandas as pd
from io import BytesIO
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# S3 config
BUCKET = "koffee-store"

logger.info("Script started")

def get_latest_bronze_file(bucket, prefix):
    try:
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        if 'Contents' not in response:
            raise Exception(f"No files found at s3://{bucket}/{prefix}")
        
        files = response['Contents']
        latest = sorted(files, key=lambda x: x['LastModified'], reverse=True)[0]
        logger.info(f"Latest bronze file: {latest['Key']}")
        return latest['Key']
    except Exception as e:
        logger.error(f"Failed to get latest bronze file: {e}")
        raise

def read_csv_from_s3(bucket, key):
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(response['Body'])
        logger.info(f"Read {len(df)} rows from S3")
        return df
    except Exception as e:
        logger.error(f"Failed to read CSV: {e}")
        raise

def transform(df):
    try:
        # rename columns
        df = df.rename(columns={
            'date': 'sale_date',
            'datetime': 'sale_datetime',
            'money': 'amount'
        })
        
        # lowercase column names
        df.columns = df.columns.str.lower().str.strip()
        
        # convert date columns
        df['sale_date'] = pd.to_datetime(df['sale_date']).dt.date
        df = df.drop('sale_datetime', axis=1)
        
        # clean string columns
        df['coffee_name'] = df['coffee_name'].str.lower().str.strip()
        df['cash_type'] = df['cash_type'].str.lower().str.strip()
        
        # drop duplicates
        df = df.drop_duplicates()
        
        logger.info(f"Transformation complete. Rows: {len(df)}")
        return df
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise

def save_parquet_to_s3(df, bucket, key):
    try:
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
        logger.info(f"Saved parquet to S3: {key}")
    except Exception as e:
        logger.error(f"Failed to save parquet: {e}")
        raise

def main():
    # dynamically pick latest bronze file
    bronze_key = get_latest_bronze_file(BUCKET, "bronze/")
    
    # read
    df = read_csv_from_s3(BUCKET, bronze_key)
    
    # transform
    df = transform(df)
    
    # save to silver with timestamp
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    silver_key = f"silver/data_{timestamp}.parquet"
    save_parquet_to_s3(df, BUCKET, silver_key)
    
    logger.info("Pipeline complete")

main()    #testing CI/CD workings