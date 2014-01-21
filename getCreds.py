import boto

try:
    from secret import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
    TOKEN = None
except:
    try:
        sts = boto.connect_sts()
        ar = sts.assume_role(role_arn="arn:aws:iam::710599580852:role/remote", role_session_name="rymurr")
        AWS_ACCESS_KEY_ID = ar.credentials.access_key
        AWS_SECRET_ACCESS_KEY = ar.credentials.secret_key
        TOKEN = ar.credentials.session_token
    except:
        TOKEN = None
        AWS_ACCESS_KEY_ID = None
        AWS_SECRET_ACCESS_KEY = None

conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, security_token=TOKEN)
bucket = conn.get_bucket('0NK38GF20CCT6VYTBG02'.lower() + '-keys')
bucket.get_key('client.key').get_contents_to_filename('client.key')
bucket.get_key('client.crt').get_contents_to_filename('client.crt')
bucket.get_key('myca.crt').get_contents_to_filename('myca.crt')
