import tweepy
from kafka import KafkaProducer
import datetime

consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

startDate = datetime.datetime(2020,3, 10, 0, 0, 0)
endDate =   datetime.datetime(2020,3, 13, 0, 0, 0)

class listener(tweepy.StreamListener):
	def __init__(self, api):
		self.api = api
		self.producer = KafkaProducer(bootstrap_servers='localhost:9092')
	#def on_data(self,data):
	#	self.producer.send('twitter', value = bytes(data, "ascii"))
	def on_status(self,status):
		#if status.created_at > endDate:
		self.producer.send('twitter', value = bytes(status.text, 'utf-8'))
		#else:
		#	print(type(status.created_at))
	def on_error(self, status):
		print(status)
		return True
		

twitter_stream = tweepy.Stream(auth = api.auth, listener = listener(api))
twitter_stream.filter(locations=[-180, -90, 180, 90], languages = ['en'], is_async=True)
