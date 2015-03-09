import Pyro4
from node import Node

class ResourceManager(Node):
    def __init__(self, oid, name="RM"):
        Node.__init__(self, oid, name)
        print 'rm %s created with id %d' %(name, oid)

    def assignjob(self, assignee, job):

    	ns = Pyro4.locateNS()
    	asgn = Pyro4.Proxy(assignee)

    	print 'got job %s from %s' %(job, asgn.tostr())

    	asgn.receivereport(self.uri, job)

    #Update the node details and job completion /failure details to the parent GS
    def updateDistributedGridScheduler():
    	return true

    #maintain the job queue for the RM 
    def rmQueue():
    	return true



 #Incase if we get time
 #class secondaryResourceManager () :




