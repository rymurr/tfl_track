import glob
import datetime

import xmltodict
import pandas

from pandas.io.pytables import HDFStore
from sqsController import recieveFromSqs, sendToSqs

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
        dfs = []
        for filename in filenames:
            with open(filename) as f:
                data = f.read()
                try:
                    doc = xmltodict.parse(data[6:])
                    df = self.parseToDataFrame(doc)
                    dfs.append(df)
                except:
                    continue
        dfd = pandas.concat(dfs)
        dfd['dateTime'] = datetime.datetime.now()
        return dfd

    def toHDF(self, hdfstore, df, name):
        for i in df.dtypes[df.dtypes==object].index:
            df[i] = df[i].map(lambda x:convert(x))
        dfx = df.dropna(how='all', axis=1)    
        try:
            store = HDFStore(hdfstore)
            store.append(name, dfx)
            store.close()
        except:
            import pdb;pdb.set_trace()
                
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
