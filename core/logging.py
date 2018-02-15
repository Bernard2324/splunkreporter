"""
Usage: Some Other File

from logging import SplunkLog

@SplunkLog()
def myfunction():
	pass

"""

import logging


class LogMeta(type):
	
	def __new__(cls, name, parents, dct):
		if not hasattr(cls, 'loglevels'):
			cls.loglevels = {
				'debug': logging.debug,
				'error': logging.error,
				'info': logging.info,
				'warning': logging.warning,
				'critical': logging.critical
			}
		
		if 'file' in dct:
			filename = dct['file']
			dct['file'] = open(filename, 'w+')
		super(LogMeta, cls).__new__(cls, name, parents, dct)
		

class SplunkLog(object):
	
	__metaclass__ = LogMeta
	file = '/var/log/splunk/splunkq.log'
	
	def __init__(self, loglevel='debug'):
		self.loggerlevel = self.loglevels.get(loglevel)
		self.splunklogger = logging.Logger()
		self.splunklogger.setLevel(self.loggerlevel)
	
	@property
	def something(self):
		pass
