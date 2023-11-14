import boto3
import logging
import requests
from requests_aws4auth import AWS4Auth
import os
import urllib.request
import base64

region = 'us-east-1'
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

host = 'https://' + os.environ["HOST"]
index = 'photos'
datatype = '_doc'
url = host + '/' + index + '/' + datatype

headers = {"Content-Type": "application/json"}

# Instantiate logger
logger = logging.getLogger()

# connect to the Rekognition client
rekognition = boto3.client('rekognition')


def image_recog(record):
    labels = []
    try:
        image = None
        s3 = boto3.resource('s3')
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        s3_object = s3.Object(bucket, key)
        # image = s3_object.get()['Body'].read()
        # print(image)
        LINK = f"https://{bucket}.s3.amazonaws.com/{key}"
        page = urllib.request.urlopen(LINK).read()
        img_data = page.decode("ascii")
        img_data = img_data.replace("data:image/jpeg;base64,", "")
        img_data = img_data.replace("'", "")
        image = base64.b64decode(img_data)
        print(image)

        response = rekognition.detect_labels(Image={'Bytes': image})
        labels = [label['Name'] for label in response['Labels']]

    except:
        print("label detection failed")

    return labels


def get_meta(record):
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']
    s3 = boto3.client('s3')
    response = s3.head_object(Bucket=bucket, Key=key)
    # TODO
    print(response["Metadata"])
    customLabels = response['Metadata'].get("customlabels", [])
    if customLabels:
        customLabels = customLabels.replace(" ", "")
        customLabels = customLabels.split(",")
        print(f"custom labels are: {customLabels}")
    json_obj = {
        "objectKey": key,
        "bucket": bucket,
        "createdTimestamp": record["eventTime"],
        "labels": customLabels
    }
    return json_obj


def lambda_handler(event, context):
    for record in event['Records']:
        print(record)
        # detect labels in the image
        detected_labels = image_recog(record)
        if not detected_labels:
            print("No labels detected")
        else:
            print("Labels found:")
            print(detected_labels)

        # retrieve metadata of the image
        json_obj = get_meta(record)

        # append detected labels to the labels field
        for lb in detected_labels:
            if lb not in json_obj["labels"]:
                json_obj["labels"].append(lb)

        print("Metadata of the image:")
        print(json_obj)

        # load data to opensearch
        r = requests.post(url, auth=awsauth, json=json_obj, headers=headers)
        print(r.text)
