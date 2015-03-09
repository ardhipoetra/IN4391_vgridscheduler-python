import Pyro4

class Job(object):
	def __init__(self, jid, name, duration):
		self.jid = jid
		self.name = name
		self.duration = duration

	def getduration(self):
		return self.duration

	def getid(self):
		return self.jid

	def getname(self):
		return self.name

	def __str__(self):
		return "([%d] Job : %s; Duration: %d ms)" %(self.jid, self.name, self.duration)