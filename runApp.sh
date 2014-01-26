#!/bin/bash

#get server/client credentials from S3
python getCreds.py

#put this hostname into the list of backups, add the backup method
curl --key client.key --cert client.crt --cacert server.pem -L https://$ETCDHOST:$ETCDPORT/v2/keys/backups/$HOSTNAME -XPUT -d value=22 -k
#put this hostname in the ghost-cort section of the hipache config
curl --key client.key --cert client.crt --cacert server.pem -L https://$ETCDHOST:$ETCDPORT/v2/keys/hipache/twilio/$HOSTNAME -XPUT -d value=80 -k
#get SES config from etcd
secret=`curl --key client.key --cert client.crt --cacert server.pem -L https://$ETCDHOST:$ETCDPORT/v2/keys/tflAWSSecret -k|cut -d':' -f5|cut -d'"' -f2`
key=`curl --key client.key --cert client.crt --cacert server.pem -L https://$ETCDHOST:$ETCDPORT/v2/keys/tflAWSKey -k|cut -d':' -f5|cut -d'"' -f2`

sed "s|AWSSECRET|$secret|" settings.py.template | sed "s|AWSKEY|$key|" > app/settings.py

supervisord -n


