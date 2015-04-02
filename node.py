import Pyro4

class Node(object):
    uri = ""

    def __init__(self, oid, name="node"):
        self.oid = oid
        self.name = name

    def getname(self):
        return self.name

    def getoid(self):
        return self.oid

    def __str__(self):
        return "[%d] %s" %(self.oid, self.name)

    def tostr(self):
        return "[%d] %s" %(self.oid, self.name)

    def assignjob(self,assignee, job):
    	print 'default job submission'

    def seturi(self, uri):
        self.uri = uri

    @Pyro4.oneway
    def test_con(self):
        pass
