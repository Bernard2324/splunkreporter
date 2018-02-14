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
		self.splunkconn = client.connect(host=self.host, port=self.port, username=self.user, \
		                                 password =self.passwd)
		
		