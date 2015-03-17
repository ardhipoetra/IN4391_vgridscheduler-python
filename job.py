import Pyro4

class Job(object):

	jid = 0
	name = 'nameofthejob'
	duration = 100

	#need to initiate these var
	RM_assigned = 0
	GS_assignee = 1

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
