import subprocess
import glob
import tarfile
import datetime
import os

from logbook import Logger
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from settings import AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID
from sqsController import recieveAllFromSqs, sendToSqs


TAR_DIR='data/tarballs/'
HDF_DIR="data/hdf/"

log = Logger(__name__)

def keyName(filename):
    sName = filename.split('/')[-1]
    if 'bz2' in sName:
        return 'xml/' + sName
    elif 'h5' in sName:
        return 'hdf/' + sName
    return sName

def uploadToS3():
    filenames = recieveAllFromSqs('tfl_queue_upload')
    log.info('Uploading to S3 {0} files'.format(len(filenames)))
    log.debug('Filenames are: {0}'.format(filenames))
    conn = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    bucket = conn.get_bucket('0nk38gf20cct6vytbg02_tfl_data')
    for filename in filenames:
        try:
            if not os.path.isfile(filename):
                log.error('Unable to fine file with filename {0}'.format(filename))
                continue
            key = Key(bucket)
            key.key = keyName(filename)
            log.debug('Sending file {0} with key {1}'.format(filename, keyName(filename)))
            key.set_contents_from_filename(filename)
            log.debug('Deleting filename {0}'.format(filename))
            os.unlink(filename)
        except:
            log.exception('Unable to upload file {0}'.format(filename))

def checkBase(path):
    if not os.path.exists(path):
        os.makedirs(path)
    else:
        assert os.path.isdir(path)

def getHDFStore():
    hdf = os.path.join(HDF_DIR, 'store.h5')
    checkBase(HDF_DIR)
    return hdf

def midnightRollHDF():
    olddir = os.path.join(HDF_DIR, 'store.h5')
    newdir = os.path.join(HDF_DIR, datetime.datetime.now().strftime('%Y%m%d-%H%M%S.h5'))
    #set indicies
    log.info('Compressing hdf file {0} to new hdf file {1}'.format(olddir, newdir))
    process = subprocess.Popen(("ptrepack", "--chunkshape=auto", "--propindexes", "--complevel=9", "--complib=bzip2", olddir, newdir), stdout=subprocess.PIPE)
    log.info('compression is complete with message {0}'.format(process.communicate()[0]))
    sendToSqs([newdir], 'tfl_queue_upload')
    log.info('New compressed hdf file sent to upload, deleting old hdf')
    try:
        if os.path.isfile(olddir):
            os.unlink(olddir)
    except:
        log.exceptio('Delete of old hdf failed!')

