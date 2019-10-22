import boto3
from botocore.exceptions import ClientError
import sys

def upload_file(bucket, file_name, object_name=None):
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    bucket.upload_file(file_name, Key=object_name)


def put_lifecycle_conf(bucket_name):
    # To set a lifecycle configuration, which says --
    #    For objects with name prefix "/logs/",      delete previous versions after 30 days
    #    For objects with name prefix "/tmp/",       delete objects after 10 days
    #    For objects with name prefix "/tmp/today/", delete previous versions after 1  day
    s3.meta.client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration={
            'Rules': [
                {
                    'ID': 'rule1',
                    'Filter': {
                        'Prefix': '/logs/',
                    },
                    'Status': 'Enabled',
                    'NoncurrentVersionExpiration': {
                        'NoncurrentDays': 30
                    },
                },
                {
                    'ID': 'rule2',
                    'Filter': {
                        'Prefix': '/tmp/',
                    },
                    'Status': 'Enabled',
                    'NoncurrentVersionExpiration': {
                        'NoncurrentDays': 10
                    },
                },
                {
                    'ID': 'rule3',
                    'Filter': {
                        'Prefix': '/tmp/today/',
                    },
                    'Status': 'Enabled',
                    'NoncurrentVersionExpiration': {
                        'NoncurrentDays': 1
                    },
                }
            ]
        }
    )

# load credentials from profile
session = boto3.Session()

# connect to server
s3 = boto3.resource('s3', use_ssl=False, endpoint_url='http://10.226.224.247')
# s3 = boto3.resource(service_name='s3', use_ssl=False, aws_access_key_id='<YOUR_ACCESS_KEY>', aws_secret_access_key='YOUR_SECRET_KEY', endpoint_url='YOUR_DATA_VIP')

BUCKET_NAME = 'yifeng-demo'
mybucket = s3.Bucket(BUCKET_NAME)
mybucket.create()

# Reset bucket
forDeletion = [{'Key':'hello.txt'}, {'Key':'logs/hello.txt'}, {'Key':'tmp/hello.txt'}, {'Key':'tmp/today/hello.txt'}]
mybucket.delete_objects(
    Delete={
        'Objects': forDeletion
    }
)

keys = [
    'logs/hello.txt', 'tmp/hello.txt', 'tmp/today/hello.txt'
]

# Create some objects
for key in keys:
    upload_file(mybucket, 'hello.txt', object_name=key)

# List objects
for obj in mybucket.objects.all():
    print(obj)

# To set lifecycle configuration
put_lifecycle_conf(BUCKET_NAME)

# To get the lifecycle configuration
lifecycle_conf = s3.meta.client.get_bucket_lifecycle_configuration(Bucket=BUCKET_NAME)
print(lifecycle_conf)

# To delete the lifecycle configuration
# s3.meta.client.delete_bucket_lifecycle(Bucket=BUCKET_NAME)
