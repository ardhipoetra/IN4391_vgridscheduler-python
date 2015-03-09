import Pyro4
import time
from node import Node
from job import Job
from constant import Constant

class WorkerNode(Node):
    def __init__(self, oid, name="WK"):
        Node.__init__(self, oid, name)
        self.status = Constant.WORKER_STATUS_IDLE


    def startjob(self, job):
    	self.status = Constant.WORKER_STATUS_BUSY

    	time.sleep(job.getduration())

    	self.status = Constant.WORKER_STATUS_IDLE