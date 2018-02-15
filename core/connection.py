# Connection to Splunk!

import redis
import subprocess
from splunklib import client
from Exceptions import *

redis_available = True
if not all(['check_output' in subprocess.__dict__.keys(),
            subprocess.check_output(['pidof', 'redis']) is not None]):
	
	redis_available = False
	
class SplunkConnectMeta(type):
	
	def __new__(cls, name, bases, dct):
		if not hasattr(cls, 'connectionregister'):
			cls.connectionregister = {}
		else:
			conninterface = name.lower()
			cls.connectionregister[conninterface] = cls
		
		super(SplunkConnectMeta, cls).__init__(name, bases, dct)

class SplunkConnection(object):
	__metaclass__ = SplunkConnectMeta
	
	def __init__(self, *args, **kwargs):
		if not all([i in kwargs for i in args]):
			raise SplunkConnectAttributeError('')
		for j in args:
			setattr(self, j, kwargs[j])
		
		if redis_available:
			# store vals for session use
			self.redise = redis.Redis(host='localhost', port=10000, db=0)
			self.redise.hmset(dict(kwargs), self.user)
			
		self.splunkconn = client.connect(host=self.host, port=self.port, username=self.user,
			password=self.passwd
		)
		
	def KeepAlive(self):
		try:
			if hasattr(self, 'splunkconn'):
				# if you import the entire splunklib, you must use splunklib.client.service
				if isinstance(self.splunkconn, client.Service):
					self.splunkconn.restart(timeout=120)
				else:
					raise SplunkConnectInstanceError('')
		except SplunkConnectInstanceError:
			creds = self.redise.hgetall()
			self.__class__(*['host', 'port', 'user', 'passwd'], **creds)
