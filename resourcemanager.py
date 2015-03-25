import Pyro4
import serpent
from constant import Constant
from worker import WorkerNode
from node import Node
import sys
import signal
import random
import utils
from Queue import Queue
stop = True;

class ResourceManager(Node):

    # list of jobs that waiting in RM before assigned to node
    job_queue = Queue()

    # id node as index,  node as val
    nodes_stat = [None] * Constant.TOTAL_NODE_EACH

    # id node, jobs on that node. Assumption, 1 node 1 job, 1 job can be > 1 node
    jobs_assigned_node = []

    def _write(self, string):
        utils.write(Constant.NODE_RESOURCEMANAGER, self.oid, string)

    def __init__(self, oid, name="RM", nodeamount=Constant.TOTAL_NODE_EACH):
        Node.__init__(self, oid, name)
        utils.write(Constant.NODE_RESOURCEMANAGER, self.oid, "created with id %d" %(oid))
        self.nodes_stat = [i for i in range(0, Constant.TOTAL_NODE_EACH)]
        self.jobs_assigned_node = [i for i in range(0, Constant.TOTAL_NODE_EACH)]
        self._creating_wnodes(nodeamount)
        self.job_queue = Queue()


    #Activity : add the incoming jobs to the local queue if all the nodes are occupied. Then wait and monitor the system
    @Pyro4.oneway
    def add_job(self,d_job):
        job = serpent.loads(d_job)

        self._write("get job {%s} added" %(job))

        self.job_queue.put(job, True, 2)

        jobhead = self._choose_job()
        nodetosubmit = self._choose_nodes()

        if nodetosubmit is not None and jobhead is not None:
            self._assignjob(nodetosubmit, jobhead)
        elif jobhead is not None:
            self.job_queue.put(jobhead, True, 2)
            self._write("WAITING for available node {%s}" %(jobhead))
        elif nodetosubmit is not None:
            self.job_queue.put(jobhead, True, 2)
            self._write("WAITING for available job {%s}" %str(nodetosubmit))


    # receive report from node
    @Pyro4.oneway
    def receive_report(self, node_id, d_job):
        job = serpent.loads(d_job)

        #what happen if the job finish?
        self.jobs_assigned_node[node_id - self.oid * 10000] = None
        self._write("get report from node %d : %s" %(node_id, str(job)))

        ns = Pyro4.locateNS(host=Constant.IP_GS_NS)
        try:
            uri = ns.lookup(Constant.NAMESPACE_GS+"."+"[GS-"+str(job["GS_assignee"])+"]"+str(job["GS_assignee"]))
            with Pyro4.Proxy(uri) as gsobj_r:
                gsobj_r.receive_report(self.oid, d_job)
        except Pyro4.errors.NamingError as e:
            self._write("CAN'T REACH GS, IGNORE REPORT TO GS %d" %job["GS_assignee"])

        if not self.job_queue.empty():
            self._write("queue not empty, try assign job to nodes")
            ajob = self._choose_job()
            nodetosubmit = self._choose_nodes()

            if nodetosubmit is not None and ajob is not None:
                self._assignjob(nodetosubmit, ajob)
            else:
                self.job_queue.put(ajob, True, 2)

    def get_cluster_info(self):
        buff = ""
        for (idx, n) in enumerate(self.nodes_stat):
            buff += str(n) + "\n"
        return buff

    def get_job_node(self):
        buff = ""
        for (idx, n) in enumerate(self.jobs_assigned_node):
            if (n is not None):
                buff += str(n) + "\n"
        return buff

    # assign job to node
    @Pyro4.oneway
    def _assignjob(self, wnode, job):
        self.jobs_assigned_node[wnode.getoid() - self.oid * 10000] = job
        self._write(str(wnode)+' assigned job '+ str(job))
        wnode.startjob(job, self)



    ###Activity : this function takes out the high prioirity job for the RM Queue
    ## output : the latest job
    def _choose_job(self):
        try:
            sjob = self.job_queue.get(True, 2)
            return sjob
        except Empty:
            self._write("q was empty!! %d" % self.job_queue.qsize())
            return None

    # choose nodes available
    def _choose_nodes(self):
        for (idx, n) in enumerate(self.nodes_stat):
            status = n.checkstatus()
            if status == Constant.WORKER_STATUS_IDLE:
                return n

        return None

    @Pyro4.oneway
    def _creating_wnodes(self, n):
        for x in range(0,n):
            n_oid = self.oid * 10000 + x
            node = WorkerNode(n_oid, "[WK-"+str(n_oid)+"]")
            self.nodes_stat[x] = node

    ## Send the current workload status to the GS send the RM id and the workload
    #how to get the object id of the resournce manager globally  ??
    def get_workloadRM(self):
        count = 0
        x = [0.0] * len(self.nodes_stat)
        for wr in self.nodes_stat:
            if wr is not None:
                x[count] = wr.getload()
            count+=1

        return sum(x)/float(len(x))

    def get_jobq(self):
        buff = "["
        for elem in list(self.job_queue.queue):
            buff += elem["jid"]+","
        buff+="]\n"
        return buff

def check_stop():
    return stop

def main():

    ns = Pyro4.locateNS(host=Constant.IP_RM_NS)

    if len(sys.argv) == 0:
        oid = len(ns.list(prefix=Constant.NAMESPACE_RM+"."))
    else:
        oid = int(sys.argv[1])

    node = ResourceManager(oid, "[RM-"+str(oid)+"]", Constant.TOTAL_NODE_EACH)

    daemon = Pyro4.Daemon(Constant.IP_RM_NS)
    uri = daemon.register(node)
    node.seturi(uri)

    ns.register(Constant.NAMESPACE_RM+"."+node.getname()+str(oid), uri)

    def signal_handler(signal, frame):
        utils.write(Constant.NODE_RESOURCEMANAGER, oid, "RM will down!")
        stop = False
        daemon.shutdown()

    signal.signal(signal.SIGINT, signal_handler)

    try:
        daemon.requestLoop(loopCondition=check_stop)
    finally:
        ns.remove(name=Constant.NAMESPACE_RM+"."+node.getname()+str(oid))
        daemon.shutdown()

if __name__=="__main__":
    main()
