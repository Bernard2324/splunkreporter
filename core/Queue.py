import pika
from Exceptions import SplunkArgumentError


def queue_callback(ch, method, properties, body):
	# carry out queue task
	
	ch.basic_ack(delivery_tag=method.delivery_tag)

"""
I decided to use a Singleton to refer to the same Queue instance accross the entire application
"""


# RabbitMQ Singleton Metaclass
class SplunkQueueMeta(type):
	_instances = {}
	
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
	def __init__(self): pass
	
	@classmethod
	def create_exchange(cls, name='splunklogs', etype='fanout'):
		cls.l_instance = SplunkQueueProperties()
		cls.l_instance.channel.exchange_declare(exchange=name, exchange_type=etype)
		

SplunkQueueExchange().create_exchange()


# RabbitMQ Close Connection
class SplunkQueueClose(object):

	@classmethod
	def queue_close(cls):
		cls.l_instance = SplunkQueueProperties()
		cls.l_instance.conn.close()
		

# RabbitMQ Producer
class SplunkQueueSend(object):
	# Generate Task
	
	@classmethod
	def send_declare(cls):
		cls.l_instance = SplunkQueueProperties()
		cls.l_instance.channel.queue_declare(queue='Splunk', durable=True)
	
	def splunk_produce(self, *args, **kwargs):
		if not any([i in kwargs for i in args]):
			raise SplunkArgumentError('')
		
		# Persistent Messaging
		self.l_instance.channel.basic_publish(
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
	
	@classmethod
	def splunk_consume(cls):
		cls.l_instance = SplunkQueueProperties()
		cls.l_instance.channel.queue_declare(queue='Splunk', durable=True)
		cls.l_instance.channel.basic_qos(prefetch_count=2)
		
		cls.l_instance.channel.basic_consume(queue_callback, queue='Splunk', no_ack=False)
		
		# Enter Consuming Loop
		cls.l_instance.channel.start_consuming()


class SplunkQueueLiveLogsProduce(object):
	
	@classmethod
	def splunk_log_produce(cls, *args, **kwargs):
		if not any([i in kwargs for i in args]):
			raise SplunkArgumentError('')
		
		cls.l_instance = SplunkQueueProperties()
		cls.l_instance.channel.basic_publish(exchange='splunklogs', routing_key='', body=kwargs['data'])
		

class SplunkQueueLiveLogsConsume(object):
	
	@classmethod
	def splunk_log_consume(cls):
		cls.l_instance = SplunkQueueProperties()
		
		livelogs = cls.l_instance.channel.queue_declare(exclusive=True)
		cls.l_instance.channel.queue_bind(exchange='logs', queue=livelogs.method.queue)
		
		cls.l_instance.channel.basic_consume(queue_callback, queue=livelogs.method.queue, no_ack=True)
		cls.l_instance.channel.start_consuming()
		