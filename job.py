import Pyro4

class Job(object):

	jid = 0
	name = 'nameofthejob'
	duration = 10 # in seconds
	load = 0.6 # 0.0 - 1.0

	#need to initiate these var
	RM_assigned = 0
	GS_assignee = 1

	def __init__(self, jid, name, duration, load, gsid):
		self.jid = jid
		self.name = name
		self.duration = duration
		self.load = load
		self.GS_assignee = gsid

	def getduration(self):
		return self.duration

	def getid(self):
		return self.jid

	def getname(self):
		return self.name

	def __str__(self):
		return "([%d] Job : %s; Duration: %d s; Take load %f)" %(self.jid, self.name, self.duration, self.load)
