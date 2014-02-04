import glob
import datetime

import xmltodict

from logbook import Logger
from sqsController import recieveAllFromSqs, sendToSqs

log = Logger(__name__)

class Base(object):
    
    def __init__(self, hdf, name, qname):
        self.hdf = hdf
        self.name = name
        self.qname = qname

    def parseToDataFrame(self, doc):
        return pandas.DataFrame()

    def extractAll(self, filename):
        if len(filename) == 0:
            return
        log.info('Extracting records from {0} xml files'.format(len(filename)))
        dfs = []
        log.debug('Read {0} lines from filename {1}'.format(len(data), filename))
        try:
            log.debug('parsing to dict')
            doc = xmltodict.parse(filename)
            log.debug('parsing to dataframe')
            df = self.parseToDataFrame(doc)
            log.debug('Parsed {0} records from file {1}'.format(len(df), filename))
            df['dateTime'] = datetime.datetime.now()
            return df
        except:
            log.exception('Exception from parsing of filename {0}'.format(filename))
            return None

#    def toHDF(self, hdfstore, df, name):
#        log.info('Creating HDF file from {0} records'.format(len(df)))
#        for i in df.dtypes[df.dtypes==object].index:
#            df[i] = df[i].map(lambda x:convert(x))
#        dfx = df.dropna(how='all', axis=1)    
#        try:
#            store = HDFStore(hdfstore)
#            store.append(name, dfx, min_itemsize = 75)
#            store.close()
#            log.debug('HDF file creation completed successfully')
#        except:
#            log.exception('hdf creation failed')
                
    def __call__(self, xml):
#        filenames = recieveAllFromSqs('tfl_queue_xml_'+self.qname)
#        if filenames is None:
#            return
        df = self.extractAll(xml)
        if df is not None and len(df) > 0:
            self.toHDF(self.hdf, df, self.name)


def convert(x):
    if isinstance(x,unicode):
        return x.encode('ascii', 'ignore')
    return x
