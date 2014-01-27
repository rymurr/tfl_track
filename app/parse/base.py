import glob
import datetime

import xmltodict
import pandas

from logbook import Logger
from pandas.io.pytables import HDFStore
from sqsController import recieveFromSqs, sendToSqs

log = Logger(__name__)

class Base(object):
    
    def __init__(self, hdf, name, qname):
        self.hdf = hdf
        self.name = name
        self.qname = qname

    def parseToDataFrame(self, doc):
        return pandas.DataFrame()

    def extractAll(self, filenames):
        if len(filenames) == 0:
            return
        log.info('Extracting records from {0} xml files'.format(len(filenames)))
        log.debug('Filenames are {0}'.format(filenames))
        dfs = []
        for filename in filenames:
            with open(filename) as f:
                data = f.read()
                log.debug('Read {0} lines from filename {1}'.format(len(data), filename))
                try:
                    log.debug('parsing to dict')
                    doc = xmltodict.parse(data[6:])
                    log.debug('parsing to dataframe')
                    df = self.parseToDataFrame(doc)
                    log.debug('Parsed {0} records from file {1}'.format(len(df), filename))
                    dfs.append(df)
                except:
                    log.exception('Exception from parsing of filename {0}'.format(filename))
        log.info('Concatenating {0} dataframes'.format(len(dfs)))            
        dfd = pandas.concat(dfs)
        dfd['dateTime'] = datetime.datetime.now()
        log.info('Returning {0} records'.format(len(dfd)))
        return dfd

    def toHDF(self, hdfstore, df, name):
        log.info('Creating HDF file from {0} records'.format(len(df)))
        for i in df.dtypes[df.dtypes==object].index:
            df[i] = df[i].map(lambda x:convert(x))
        dfx = df.dropna(how='all', axis=1)    
        try:
            store = HDFStore(hdfstore)
            store.append(name, dfx, min_itemsize = 75)
            store.close()
            log.debug('HDF file creation completed successfully')
        except:
            log.exception('hdf creation failed')
                
    def __call__(self):
        filenames = recieveFromSqs('tfl_queue_xml_'+self.qname)
        if filenames is None:
            return
        df = self.extractAll(filenames.split(','))
        if df is not None:
            self.toHDF(self.hdf, df, self.name)
            return sendToSqs(filenames.split(','), 'tfl_queue_tar')


def convert(x):
    if isinstance(x,unicode):
        return x.encode('ascii', 'ignore')
    return x
