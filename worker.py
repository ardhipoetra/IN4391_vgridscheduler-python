import Pyro4
import time
from node import Node
from job import Job
from constant import Constant
import threading
import serpent
import utils

class WorkerNode(Node):

    def _write(self, string):
        utils.write(Constant.NODE_WORKER, self.oid, string)

    def __init__(self, oid, name="WK"):
        Node.__init__(self, oid, name)
        self.status = Constant.WORKER_STATUS_IDLE
        self.load = 0.0

    def startjob(self, job_obj, rmobj):
        self._write('job %s started' %str(job_obj))
    	self.status = Constant.WORKER_STATUS_BUSY

        def do_job(dur, jobj, load, rmid, lermobj):
            self.load = load
            time.sleep(dur)
            self.status = Constant.WORKER_STATUS_IDLE
            self.load = 0.0

            try:
                ns = Pyro4.locateNS(host=Constant.IP_RM_NS)
                uri = ns.lookup(Constant.NAMESPACE_RM+"."+"[RM-"+str(rmid)+"]"+str(rmid))

                self._write("Job finished from worker")
                lermobj.receive_report(self.oid, serpent.dumps(jobj))

            except Pyro4.errors.NamingError as e:
                self._write("CAN'T REACH RM, IGNORE REPORT TO RM")



    	thread = threading.Thread(target=do_job, args=([job_obj["duration"],job_obj,  job_obj['load'], job_obj["RM_assigned"], rmobj]))
        thread.setDaemon(True)
        thread.start()

    def __str__(self):
        return "[%d] %s ;Load:%f; Stat:%d" %(self.oid, self.name, self.load, self.status)

    def checkstatus(self):
        return self.status

    def getload(self):
        return self.load
