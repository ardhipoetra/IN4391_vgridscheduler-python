import Pyro4

class Job(object):
	def __init__(self, jid, name, duration):
		self.jid = jid
		self.name = name
		self.duration = duration

	def getduration(self):
		return self.duration