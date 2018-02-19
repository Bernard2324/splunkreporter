import pika
from Exceptions import SplunkArgumentError

"""
I decided to use a Singleton to refer to the same Queue instance accross the entire application
"""
class SplunkQueueMeta(type):
	_instances = {}
	
	def __call__(cls, *args, **kwargs):
		if cls not in cls._instances:
			cls._instances[cls] = super(SplunkQueueMeta, cls).__call__(*args, **kwargs)
		return cls._instances[cls]


class SplunkQueueProperties(object):
	__metaclass__ = SplunkQueueMeta
	
	def __init__(self):
		self.conn = pika.BlockingConnection(pika.ConnectionParameters('localhost', 10000))
		self.channel = self.conn.channel()
		
	
class SplunkQueueClose(object):
	
	def __init__(self):
		self.l_instance = SplunkQueueProperties()
	
	@staticmethod
	def queue_close(self):
		self.l_instance.conn.close()
		

class SplunkQueueSend(object):
	
	def __init__(self):
		self.l_instance = SplunkQueueProperties()
		
	@staticmethod
	def send_declare(self):
		self.l_instance.channel.queue_declare(queue='Splunk')
		
	def splunk_exchange(self, *args, **kwargs):
		if not any([i in kwargs for i in args]):
			raise SplunkArgumentError('')
		
		self.l_instance.channel.basic_publish(
			exchange='',
			routing_key='Splunk',
			body=kwargs['data']
		)
	

class SplunkQueueReceive(object):
	
	def __init__(self):
		self.l_instance = SplunkQueueProperties()
		
	@staticmethod
	def send_declare(self):
		self.l_instance.channel.queue_declare(queue='Splunk')