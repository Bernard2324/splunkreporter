import threading

from logging import *
from Queue import *
from Exceptions import *


class SplunkThread(threading.Thread):
	
	def __init__(self, *args, **kwargs):
		
		if not all([i in kwargs for i in args]):
			raise SplunkArgumentError('')
		for i in args:
			setattr(self, i, kwargs[i])
		
		self.queryqueue = SplunkQueue()
		self.log = SplunkLog
		super(SplunkThread, self).__init__()
	
	def spawn(self):
		SplunkLog.splunklogger("Starting {}".format(self.name))
		
		self.process_data(self.name, self.queryqueue)
		SplunkLog.splunklogger("Exiting {}".format(self.name))
		
	def process_data(self, name, queue):
		while not exitFlag:
			threading.Lock()
			if self.queryqueue is not None:
				data = self.queryqueue.get()
				threading.release()
				SplunkLog.splunklogger("processing {}".format(name, data))
			else:
				threading.release()