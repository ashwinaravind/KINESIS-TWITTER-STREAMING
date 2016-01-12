from __future__ import print_function
import sys
import boto
import threading
import time
import datetime
import json
from boto.kinesis.exceptions import ProvisionedThroughputExceededException
import botoProducer
from random import randint

iter_type = 'TRIM_HORIZON'

class TWTKinesisWorker(threading.Thread):
    def __init__(self, stream_name, shard_id, iterator_type,
                 worker_time=30, sleep_interval=0.5,name=None):
        super(TWTKinesisWorker, self).__init__(name=name)
        self.stream_name = stream_name
        self.shard_id = str(shard_id)
        self.iterator_type = iterator_type
        self.worker_time = worker_time
        self.sleep_interval = sleep_interval
        self.total_records = 0

    def run(self):
        TWTfile=open("TWT.txt","w")
        my_name = threading.current_thread().name
        print ('+ KinesisWorker:', my_name)
        print ('+-> working with iterator:', self.iterator_type)
        response = kinesis.get_shard_iterator(self.stream_name,self.shard_id, self.iterator_type)
        next_iterator = response['ShardIterator']
        print ('+-> getting next records using iterator:',shard_id ,next_iterator)
        start = datetime.datetime.now()
        finish = start + datetime.timedelta(seconds=self.worker_time)
        while finish > datetime.datetime.now():
            try:
                response = kinesis.get_records(next_iterator)
                self.total_records += len(response['Records'])
                for record in response['Records']:
                    text = record['Data']
                    t=json.dumps(text)
                    TWTfile.writelines("------------------------------------------------------------------------\n")
                    TWTfile.writelines(t)
                if len(response['Records']) > 0:
                    print ('\n+-> {1} Got {0} Worker Records'.format(
                        len(response['Records']), my_name))
                next_iterator = response['NextShardIterator']
                time.sleep(self.sleep_interval)
            except ProvisionedThroughputExceededException as ptex:
                print (ptex.message)
                time.sleep(5)
        TWTfile.close()

if __name__ == '__main__':
    kinesis = boto.kinesis.connect_to_region(region_name = "us-east-1")
    stream = kinesis.describe_stream("TWTSTREAM")
    shards = stream['StreamDescription']['Shards']
    print ('# Shard Count:', len(shards))
    threads = []
    start_time = datetime.datetime.now()
    for shard_id in xrange(len(shards)):
        worker_name = 'Consumer for :%s' % shard_id
        print ('#-> shardId:', shards[shard_id]['ShardId'])
        worker = TWTKinesisWorker(
            stream_name="TWTSTREAM",
            shard_id=shards[shard_id]['ShardId'],
            iterator_type=iter_type,  # uses TRIM_HORIZON
            worker_time=1000,
            sleep_interval=.5,
            name=worker_name
            )
        worker.daemon = True
        threads.append(worker)
        print ('#-> starting: ', worker_name)
        worker.start()
 # Wait for all threads to complete
    for t in threads:
        t.join()
    print ("Exiting Consumer")
