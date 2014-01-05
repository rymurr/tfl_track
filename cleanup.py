import glob
import tarfile
import datetime
import os
import base

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from data import LINES, STATIONS, LINE_STATIONS
from settings import AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID


BASE_DIR='data/xml'
XML_DIR='data/xml'
BASE_DIR='data/xml'
SUFFIX='.xml'
SUMMARY_URL = 'http://cloud.tfl.gov.uk/TrackerNet/PredictionSummary/'
DETAIL_URL = 'http://cloud.tfl.gov.uk/TrackerNet/PredictionDetailed/'

def createTar():
    prefix = os.path.expanduser(BASE_DIR)
    tarfilename = prefix + datetime.datetime.today().strftime('-%Y%m%d-%H%M%S')+'.tar.bz2'
    with tarfile.open(tarfilename, 'w:bz2') as tar:
        for filename in glob.glob(os.path.join(prefix,'*.xml')):
            tar.add(filename)
    for filename in os.listdir(prefix):
        fpath = os.path.join(prefix, filename)
        try:
            if os.path.isfile(fpath):
                os.unlink(fpath)
        except:
            pass
    return tarfilename
 

def uploadToS3(filename):
    conn = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    bucket = conn.get_bucket(AWS_ACCESS_KEY_ID.lower() + '_tfl_data') 
    key = Key(bucket)
    key.key = filename
    key.set_contents_from_filename(filename)
    #TODO: I dont think this is working...FIX IT!
    try:
        if os.path.isfile(filename):
            os.unlinke(filenmae)
    except:
        pass

