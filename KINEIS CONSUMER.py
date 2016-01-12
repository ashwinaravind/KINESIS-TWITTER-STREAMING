from __future__ import print_function

import boto
import time
from boto.kinesis.exceptions import ResourceNotFoundException
import json
import tweepy
import threading
from tweepy.streaming import StreamListener
from tweepy import Stream
from random import randint
class TWTKinesisPoster(threading.Thread):

    def __init__(self,arg1 ) :
          super(TWTKinesisPoster, self).__init__()
          print ("STREAM INITIALIZED : ", arg1 )

    def run(self):
        print("STREAM STARTING ")


class StdOutListener(StreamListener):
    def on_data(self, data):
        post = json.dumps(data)
        PARTKEY="TWTSHRD"+str(randint(0,9))
        response =kinesis.put_record(stream_name="TWTSTREAM",data=data, partition_key=PARTKEY)
        print ("-= put seqNum:", response['SequenceNumber'])
        print (response)
        #stream.disconnect()
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    shard_count =5
    consumer_key = 'pqDWJt3yumzEUve9z9nYQ'
    consumer_secret = '6xZlavM3lRWvBx5UJbP0fcouRTvotfiZxJwMMgznRM'
    access_token = '144899829-oY8bLBPCrFOBBHKRYct51DYSjwKlJ26oYW8iwsHv'
    access_token_secret = 'nnbeFKE4dxjH2QGeRRjbHdrfAIj3Oj3Xyic0yCyO2KU'
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    user = api.me()
    print('CONNECTING  USER : ' + user.name)
    l = StdOutListener()
    stream = Stream(auth, l)
    kinesis = boto.kinesis.connect_to_region(region_name = "us-east-1")
    #kinesis.create_stream("TWTSTREAM", shard_count)
    poster=TWTKinesisPoster("TWTSTREAM")
    poster.daemon = True
    poster.start()
    time.sleep(30)
    stream.filter(track=["star wars"])





