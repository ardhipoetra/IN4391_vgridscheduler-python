import Pyro4
import time
from node import Node
from job import Job
from constant import Constant
import threading

class WorkerNode(Node):

    def __init__(self, oid, name="WK"):
        Node.__init__(self, oid, name)
        self.status = Constant.WORKER_STATUS_IDLE
        self.load = 0.0

    def startjob(self, job_obj):
    	self.status = Constant.WORKER_STATUS_BUSY

        def do_job(dur, load):
            self.load = load
            time.sleep(dur)
            self.status = Constant.WORKER_STATUS_IDLE
            self.load = 0.0

    	thread = threading.Thread(target=do_job, args=([job_obj["duration"], job_obj['load']]))
        thread.setDaemon(True)
        thread.start()

    def __str__(self):
        return "[%d] %s ;Load:%f; Stat:%d" %(self.oid, self.name, self.load, self.status)

    def checkstatus(self):
        return self.status

    def getload(self):
        return self.load
