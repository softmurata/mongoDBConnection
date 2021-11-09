import json
import os
import boto3
import botocore
import datetime
import pymongo
from pymediainfo import MediaInfo
from botocore.config import Config
from botocore.exceptions import ClientError

region = os.environ['AWS_REGION']

# email group
COMPANY_1 = ["gengminghuang@gmail.com"]
COMPANY_2 = ["tatsuromurata317@gmail.com"]
COMPANY_3 = ["tatsumurata321@gmail.com"]
COMPANY_4 = ["soce0209@gmail.com"]
SHARE = COMPANY_1 + COMPANY_2 + COMPANY_3 + COMPANY_4

# The character encoding for the email.
CHARSET = "UTF-8"

SUBJECT = "Amazon SES Test (SDK for Python)"

def get_signed_url(expires_in, bucket, obj):
    """
    Generate a signed URL
    :param expires_in:  URL Expiration time in seconds
    :param bucket:
    :param obj:         S3 Key name
    :return:            Signed URL
    """

    s3_cli = boto3.client("s3", region_name=region, config = Config(signature_version = 's3v4', s3={'addressing_style': 'virtual'}))
    presigned_url = s3_cli.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': obj}, ExpiresIn=expires_in)
    return presigned_url

def lambda_handler(event, context):
    tmp_filename='/tmp/my_video.mp4'
    s3 = boto3.resource('s3')

    message = json.loads(event['Records'][0]['Sns']['Message'])

    # setting at environment variables
    # BUCKET_NAME = os.environ.get("BUCKET_NAME")
    # S3_KEY = os.environ.get("S3_KEY")
    BUCKET_NAME = message['Records'][0]['s3']['bucket']['name']
    S3_KEY = message['Records'][0]['s3']['object']['key']

    try:
        s3.Bucket(BUCKET_NAME).download_file(S3_KEY, tmp_filename)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist: s3://" + BUCKET_NAME + S3_KEY)
        else:
            raise

    media_info = MediaInfo.parse(tmp_filename, library_file='/opt/libmediainfo.so.0')

    print(str(media_info.to_json()))

    for track in media_info.tracks:
        if track.track_type == 'Video':
            print("track info: " + str(track.bit_rate) + " " + str(track.bit_rate_mode)  + " " + str(track.codec))

    SIGNED_URL_EXPIRATION = 3000     # The number of seconds that the Signed URL is valid

    # Generate a signed URL for the uploaded asset
    signed_url = get_signed_url(SIGNED_URL_EXPIRATION, BUCKET_NAME, S3_KEY)
    # Launch MediaInfo
    media_info = MediaInfo.parse(signed_url, library_file='/opt/libmediainfo.so.0')
    duration = 0
    for track in media_info.tracks:
        if track.track_type == 'Video':
            print("track info: " + str(track.bit_rate) + " " + str(track.bit_rate_mode)  + " " + str(track.codec))
            duration = track.duration

    # email notification
    client = boto3.client('ses',region_name=region)
    try:
        # create body text and body html
        BODY_TEXT = ("Amazon SES Test (Python)\r\n"
             "This email was sent with Amazon SES using the "
             "AWS SDK for Python (Boto)."
            )
        # The HTML body of the email.
        
        code = signed_url
        BODY_HTML = """<html>
            <head></head>
            <body>
            <h1>Amazon S3 notification</h1>
            <p>This email was sent with
            <a href='https://aws.amazon.com/ses/'>Amazon SES</a>
            <p>Upload new {BUCKET_NAME} / {S3_KEY}</p>
            <a href={code}>S3 signed Url</a>
            </body>
            </html>
        """.format(**locals())

        # s3_key = "company-1/***.mp4"
        # if company-1 => [test@gmail.com, test1@gmail.com]
        # else if company-2 => [dggg@gmail.com]

        company_name = S3_KEY.split("/")[0]

        email_groups = SHARE

        if company_name == "company-1":
            email_groups = COMPANY_1
        elif company_name == "company-2":
            email_groups = COMPANY_2
        elif company_name == "company-3":
            email_groups = COMPANY_3
        elif company_name == "company-4":
            email_groups = COMPANY_4

        #Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': email_groups,
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source="gengminghuang@gmail.com",
        )
        # Display an error if something goes wrong.	
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])
            
    


    # connect to mongodb
    mongo_url = "mongodb+srv://murata:murata@cluster0.w5mid.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"

    client = pymongo.MongoClient(mongo_url)
    db = client["myFirstDatabase"]
    collection = db["sampleCollection"]

    # data schema
    document = {"filename":BUCKET_NAME + "/" + S3_KEY,
    "duration": duration}

    id = collection.insert_one(document).inserted_id
    print("id:", id)


    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }