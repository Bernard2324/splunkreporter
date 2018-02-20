import pika
import time
from Exceptions import SplunkArgumentError


def outbuffer(args):
	# for now I will print, later I want these in a DB for Normalization
	print(time.strftime('%Y-%m-%d %H:%M:%S'), args)
	

def queue_callback(ch, method, properties, body):
	# carry out queue task
	ret = dict()
	ret['routing_key'] = method.routing_key
	ret['headers'] = properties.headers
	ret['body'] = body
	outbuffer(ret)

"""
I decided to use a Singleton to refer to the same Queue instance accross the entire application
"""


# RabbitMQ Singleton Metaclass
class SplunkQueueMeta(type):
	_instances = dict()
	
	def __call__(cls, *args, **kwargs):
		if cls not in cls._instances:
			cls._instances[cls] = super(SplunkQueueMeta, cls).__call__(*args, **kwargs)
		return cls._instances[cls]


# RabbitMQ Connection & Channel Instance
class SplunkQueueProperties(object):
	__metaclass__ = SplunkQueueMeta
	
	def __init__(self):
		self.conn = pika.BlockingConnection(pika.ConnectionParameters('localhost', 10000))
		self.channel = self.conn.channel()
		

# RabbitMQ Exchange
class SplunkQueueExchange(object):
	# Handle Producer Messages
	
	@staticmethod
	def create_exchange(name='splunklogs', etype='fanout'):
		# hide the private variable in @staticmethod decorator, which does not pass object instance or object class
		# thus this private method cannot be accessed outside
		_l_instance = SplunkQueueProperties()
		_l_instance.channel.exchange_declare(exchange=name, exchange_type=etype)
		

SplunkQueueExchange.create_exchange()


# RabbitMQ Close Connection
class SplunkQueueClose(object):

	@staticmethod
	def queue_close():
		# Call: SplunkQueueClose.queue_close()
		
		_l_instance = SplunkQueueProperties()
		_l_instance.conn.close()


#RabbitMQ Producer
class SplunkQueueSend(object):
	# Generate Task

	# def __init__(self):
	# 	_l_instance = SplunkQueueProperties()
	# 	_l_instance.channel.queue_declare(queue='Splunk', durable=True)
	# 	self.i_access = _l_instance

	@staticmethod
	def splunk_produce(*args, **kwargs):
		_l_instance = SplunkQueueProperties()
		_l_instance.channel.queue_declare(queue='Splunk', durable=True)
		if not any([i in kwargs for i in args]):
			raise SplunkArgumentError('')

		# Persistent Messaging
		_l_instance.channel.basic_publish(
			exchange='',
			routing_key='Splunk',
			body=kwargs['data'],
			properties=pika.BasicProperties(
				delivery_mode=2
			)
		)


# RabbitMQ Consumer
class SplunkQueueReceive(object):
	# Receive and 'Work' Task
	
	@staticmethod
	def splunk_consume():
		_l_instance = SplunkQueueProperties()
		_l_instance.channel.queue_declare(queue='Splunk', durable=True)
		_l_instance.channel.basic_qos(prefetch_count=2)
		
		_l_instance.channel.basic_consume(queue_callback, queue='Splunk', no_ack=False)
		
		# Enter Consuming Loop
		_l_instance.channel.start_consuming()


class SplunkQueueLiveLogsProduce(object):
	
	@staticmethod
	def splunk_log_produce(*args, **kwargs):
		if not any([i in kwargs for i in args]):
			raise SplunkArgumentError('')
		
		_l_instance = SplunkQueueProperties()
		_l_instance.channel.basic_publish(exchange='splunklogs', routing_key='', body=kwargs['data'])
		

class SplunkQueueLiveLogsConsume(object):
	
	@staticmethod
	def splunk_log_consume():
		_l_instance = SplunkQueueProperties()
		
		livelogs = _l_instance.channel.queue_declare(exclusive=True)
		_l_instance.channel.queue_bind(exchange='logs', queue=livelogs.method.queue)
		
		_l_instance.channel.basic_consume(queue_callback, queue=livelogs.method.queue, no_ack=True)
		_l_instance.channel.start_consuming()

