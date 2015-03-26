import Pyro4
import serpent
from constant import Constant
from constant import Pool
from worker import WorkerNode
from node import Node
import sys
import signal
import random
import utils
from Queue import Queue
import socket
import subprocess

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
        self.jobs_assigned_node = [None for i in range(0, Constant.TOTAL_NODE_EACH)]
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

    def get_totaljobs_run(self):
        totnon = 0
        for ob in self.jobs_assigned_node:
            if ob is not None:
                totnon+=1

        return totnon


    def get_jobq(self):
        buff = "["
        for elem in list(self.job_queue.queue):
            buff += str(elem["jid"])+","
        buff+="]\n"
        return buff

def check_stop():
    return stop

def main():
    #check for ns :
    nsready = 0
    hostname = socket.gethostname()
    rm_ip = Pyro4.socketutil.getIpAddress(hostname, workaround127=True)

    # spawn ns, probably can't find it
    try :
        Pyro4.locateNS(host=rm_ip)
        nsready = -1
    except Pyro4.errors.NamingError:
        subprocess.Popen(["python","-m","Pyro4.naming","--host="+rm_ip])
        nsready = 1


    # ns should be available
    while(nsready > 0):
        try:
            Pyro4.locateNS(host=rm_ip)
            nsready = -1
        except Pyro4.errors.NamingError:
            time.sleep(0.5)

    #ns should be ready by now
    if len(sys.argv) != 2:
        print "you must provide RM id"
        sys.exit()
    else:
        oid = int(sys.argv[1])


    node = ResourceManager(oid, "[RM-"+str(oid)+"]", Constant.TOTAL_NODE_EACH)
    daemon = Pyro4.Daemon(rm_ip)
    uri = daemon.register(node)
    node.seturi(uri)

    def signal_handler(signal, frame):
        utils.write(Constant.NODE_RESOURCEMANAGER, oid, "RM will down!")
        stop = False
        daemon.shutdown()

    with Pyro4.locateNS(host=rm_ip) as ns:
        ns.register(Constant.NAMESPACE_RM+"."+node.getname()+str(oid), uri)

    signal.signal(signal.SIGINT, signal_handler)

    check_env()

    print "[%f]-%d RM everything ready!" %(time.time(), oid)

    try:
        daemon.requestLoop(loopCondition=check_stop)
    finally:
        ns.remove(name=Constant.NAMESPACE_RM+"."+node.getname()+str(oid))
        daemon.shutdown()

def check_env():
    # check if all GS are ready
    lgs_tmp = [None] * Constant.TOTAL_GS
    ready = False
    while(not ready):
        for i in range(0,Constant.TOTAL_GS):
            for ip in Pool.POTENTIAL_LINK:
                try:
                    Pyro4.Proxy("PYRONAME:%s.[GS-%d]%d@%s" %(Constant.NAMESPACE_GS,i,i,ip))
                    lgs_tmp[i] = ip
                except Pyro4.errors.NamingError:
                    pass


            if i == Constant.TOTAL_GS - 1:
                time.sleep(1)
                ready = (len([el for el in lgs_tmp if el is None]) == 0)

    for gid, gip in enumerate(lgs_tmp):
        if "gs-"+str(gid) not in Pool.lookuptable:
            Pool.lookuptable["gs-"+str(gid)] = gip

    # check if all RM are ready
    lrm_tmp = [None] * Constant.TOTAL_RM
    ready = False
    while(not ready):
        for i in range(0,Constant.TOTAL_RM):
            for ip in Pool.POTENTIAL_LINK:
                try:
                    Pyro4.Proxy("PYRONAME:%s.[RM-%d]%d@%s" %(Constant.NAMESPACE_RM,i,i,ip))
                    lrm_tmp[i] = ip
                except Pyro4.errors.NamingError:
                    pass


            if i == Constant.TOTAL_RM - 1:
                time.sleep(1)
                ready = (len([el for el in lrm_tmp if el is None]) == 0)

    for rid, rip in enumerate(lrm_tmp):
        if ("rm-"+str(rid)) not in Pool.lookuptable:
            Constant.lookuptable["rm-"+str(rid)] = rip


if __name__=="__main__":
    main()
