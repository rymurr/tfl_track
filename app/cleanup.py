import glob
import tarfile
import datetime
import os

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from settings import AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID


XML_DIR='data/xml/'
TAR_DIR='data/tarballs/'

def createTar():
    prefix = os.path.expanduser(TAR_DIR)
    checkBase(prefix)
    xmlprefix = os.path.expanduser(XML_DIR)
    tarfilename = prefix + datetime.datetime.today().strftime('-%Y%m%d-%H%M%S')+'.tar.bz2'
    _, dirs, _ = os.walk(XML_DIR).next()
    with tarfile.open(tarfilename, 'w:bz2') as tar:
        for directory in dirs:
            fdir = os.path.join(xmlprefix,directory)
            for filename in glob.glob(os.path.join(fdir,'*.xml')):
                tar.add(filename)
        for filename in os.listdir(fdir):
            fpath = os.path.join(prefix, filename)
            try:
                if os.path.isfile(fpath):
                    os.unlink(fpath)
            except:
                pass
    return tarfilename
 

def uploadToS3():
    conn = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    bucket = conn.get_bucket(AWS_ACCESS_KEY_ID.lower() + '_tfl_data') 
    prefix = os.path.expanduser(TAR_DIR)
    for filename in os.listdir(prefix):
        key = Key(bucket)
        key.key = filename
        fpath = os.path.join(prefix, filename)
        key.set_contents_from_filename(fpath)
        try:
            if os.path.isfile(fpath):
                os.unlink(fpath)
        except:
            pass

def checkBase(path):
    if not os.path.exists(path):
        os.makedirs(path)
    else:
        assert os.path.isdir(path)


