import Pyro4
import serpent
from node import Node

class ResourceManager(Node):
    def __init__(self, oid, name="RM"):
        Node.__init__(self, oid, name)
        print 'rm %s created with id %d' %(name, oid)

    def assignjob(self, assignee, d_job):

    	ns = Pyro4.locateNS()
    	asgn = Pyro4.Proxy(assignee)
        job = serpent.loads(d_job)

        print '%s got job %s from %s' %(self, job, asgn.tostr())
    	asgn.receivereport(self.uri, d_job)

    
    # Activity :  this function Update the node details and job completion /failure details to the parent GS
    # 
    #output : datastructure with values such as failed node id , avaiable jobs spots, job completion details.
    def update_distributedGS(self):
    	return True

    #Activity : this function add the incoming jobs to the queue if all the jobs are occupied .
    # output : do nt output anything 
    def rmQueue(self):
    	return True


    ###Activity : this function takes out the latest job for the RM Queue
    ## output : the latest job
    def  getJobFromRMQueue(self):
    	return job

 #Incase if we get time
 #class secondaryResourceManager (Node) :
    #backup stuff
