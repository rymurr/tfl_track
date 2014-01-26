import subprocess
import glob
import tarfile
import datetime
import os

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from settings import AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID
from sqsController import recieveAllFromSqs, sendToSqs


XML_DIR='data/xml/'
TAR_DIR='data/tarballs/'
HDF_DIR="data/hdf/"

def createTar():
    filenames = recieveAllFromSqs('tfl_queue_tar')
    if len(filenames) == 0:
        return
    prefix = os.path.expanduser(TAR_DIR)
    checkBase(prefix)
    tarfilename = prefix + datetime.datetime.today().strftime('%Y%m%d-%H%M%S')+'.tar.bz2'
    with tarfile.open(tarfilename, 'w:bz2') as tar:
        for filename in filenames:
            #fdir = os.path.join(xmlprefix,directory)
            #for filename in glob.glob(os.path.join(fdir,'*.xml')):
            tar.add(filename)
            #for filename in os.listdir(fdir):
            #fpath = os.path.join(fdir, filename)
            try:
                if os.path.isfile(filename):
                    os.unlink(filename)
            except:
                pass
    return sendToSqs([tarfilename], 'tfl_queue_upload')
 

def keyName(filename):
    sName = filename.split('/')[-1]
    if 'bz2' in sName:
        return 'xml/' + sName
    elif 'h5' in sName:
        return 'hdf/' + sName
    return sName

def uploadToS3():
    filenames = recieveAllFromSqs('tfl_queue_upload')
    conn = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    bucket = conn.get_bucket('0nk38gf20cct6vytbg02_tfl_data')
    for filename in filenames:
        try:
            key = Key(bucket)
            key.key = keyName(filename)
            key.set_contents_from_filename(filename)
            if os.path.isfile(filename):
                os.unlink(filename)
        except:
            pass

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
    newdir = os.path.join(HDF_DIR, datetime.datetime.now().strftime('%Y%m%d-%H%M%s.h5'))
    #set indicies
    process = subprocess.Popen(("ptrepack", "--chunkshape=auto", "--propindexes", "--complevel=9", "--complib=bzip2", olddir, newdir), stdout=subprocess.PIPE)
    print process.communicate()[0]
    sendToSqs([newdir], 'tfl_queue_upload')
    try:
        if os.path.isfile(olddir):
            os.unlink(olddir)
    except:
        pass

