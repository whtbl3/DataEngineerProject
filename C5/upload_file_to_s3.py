import boto3
import configparser
import os
import logging
from botocore.exceptions import ClientError

def config_object():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    return config

def s3_resource(config=config_object()):
    AWS_ACCESS_KEY_ID = config.get("AWS_ACCESS", "aws_access_key_id")
    AWS_SECRET_ACCESS_KEY = config.get("AWS_ACCESS", "aws_secret_access_key")
    region = config.get('AWS_ACCESS', 'AWS_REGION')
    s3 = boto3.resource('s3',
                   region_name=region,
                   aws_access_key_id=AWS_ACCESS_KEY_ID,
                   aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
               )
    return s3

def create_bucket(bucket_name, 
                  config=config_object(), 
                  s3_resource=s3_resource()):
    region = config.get('AWS_ACCESS', 'AWS_REGION')
    # Create bucket
    try:
        location = {'LocationConstraint': region}
        s3_resource.create_bucket(Bucket=bucket_name,
                         CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_immigration(bucket_name, s3_resource=s3_resource()):
    root = "../../data/18-83510-I94-Data-2016/"
    for f in os.listdir(root):
        s3_resource.Bucket(bucket_name).upload_file("{}/{}".format(root, f), "raw_data/i94_immigration/18-83510-I94-Data-2016/{}".format(f))

def upload_demographics(bucket_name, s3_resource=s3_resource()):
    fname = "us-cities-demographics.csv"
    s3_resource.Bucket(bucket_name).upload_file("dataset/{}".format(fname), "raw_data/demography/{}".format(fname))

def upload_global_temperature(bucket_name, s3_resource=s3_resource()):
    fname = "GlobalLandTemperaturesByCity.csv"
    s3_resource.Bucket(bucket_name).upload_file("../../data2/{}".format(fname), "raw_data/global_temperature/{}".format(fname))

def upload_airport_code(bucket_name, s3_resource=s3_resource()):
    fname = "airport-codes_csv.csv"
    s3_resource.Bucket(bucket_name).upload_file("dataset/{}".format(fname), "raw_data/airport/{}".format(fname))

def upload_datacode(bucket_name, s3_resource=s3_resource()):
    root = "dataset"
    for f in os.listdir(root):
        if f.startswith('i94'):
            s3_resource.Bucket(bucket_name).upload_file("dataset/{}".format(f), "raw_data/codes/{}".format(f))

    
def main():
    bucket_name = "datasetudacitycapstone"
    if not create_bucket(f"{bucket_name}"):
        create_bucket(f"{bucket_name}")
    else:
        upload_immigration(bucket_name)
        upload_demographics(bucket_name)
        upload_global_temperature(bucket_name)
        upload_airport_code(bucket_name)
        upload_datacode(bucket_name)
        print("Uploaded to S3Bucket Successfully!!!")
        
if __name__ == '__main__':
    main()