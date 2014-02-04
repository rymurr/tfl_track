import boto

from logbook import Logger

from boto.sqs.message import Message
from settings import AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID

log = Logger(__name__)

def sendToSqs(filenames, queue_name='tfl_queue_xml'):
    #TODO: check if under 256kb and split!
    fnStr = ','.join(filenames)
    sqs = boto.connect_sqs(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    q = sqs.create_queue(queue_name)
    m = Message()
    m.set_body(fnStr)
    log.info('Sending message to {0}.'.format(queue_name))
    log.debug('Message content is {0}'.format(fnStr))
    return q.write(m)

def recieveFromSqs(queue_name = 'tfl_queue_xml', delete=True):
    sqs = boto.connect_sqs(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    q = sqs.create_queue(queue_name)
    m = q.read()
    if m is not None:
        fnStr = m.get_body()
        if delete:
            q.delete_message(m)
        log.info('Recieving message from {0}.'.format(queue_name))
        log.debug('Message content is {0}'.format(fnStr))
        return fnStr

def recieveAllFromSqs(queue_name = 'tfl_queue_xml', delete=True):
    sqs = boto.connect_sqs(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    q = sqs.create_queue(queue_name)
    ms = q.get_messages()
    fns = []
    log.info('Recieving {1} messages from {0}.'.format(queue_name,len(ms)))
    for m in ms:
        fnStr = m.get_body()
        #if delete:
        #    q.delete_message(m)
        log.debug('Message content is {0}'.format(fnStr))
        fns.extend(fnStr.split(','))
    #q.clear()
    return fns    

