import Pyro4
import time
from node import Node
from job import Job

class WorkerNode(Node):

	STATUS_BUSY = 1
	STATUS_IDLE = 1

    def __init__(self, oid, name="WK"):
        Node.__init__(self, oid, name)
        self.status = STATUS_IDLE


    def startjob(self, job):
    	self.status = STATUS_BUSY

    	time.sleep(job.getduration())

    	self.status = STATUS_IDLE