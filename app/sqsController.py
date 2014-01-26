import boto

from boto.sqs.message import Message
from settings import AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID

def sendToSqs(filenames, queue_name='tfl_queue_xml'):
    #TODO: check if under 256kb and split!
    fnStr = ','.join(filenames)
    sqs = boto.connect_sqs(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    q = sqs.create_queue(queue_name)
    m = Message()
    m.set_body(fnStr)
    return q.write(m)

def recieveFromSqs(queue_name = 'tfl_queue_xml', delete=True):
    sqs = boto.connect_sqs(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    q = sqs.create_queue(queue_name)
    m = q.read()
    if m is not None:
        fnStr = m.get_body()
        if delete:
            q.delete_message(m)
        return fnStr

def recieveAllFromSqs(queue_name = 'tfl_queue_xml', delete=True):
    sqs = boto.connect_sqs(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    q = sqs.create_queue(queue_name)
    ms = q.get_messages()
    fns = []
    for m in ms:
        fnStr = m.get_body()
        #if delete:
        #    q.delete_message(m)
        fns.extend(fnStr.split(','))
    q.clear()
    return fns    

