import Pyro4
import serpent
from constant import Constant
from worker import WorkerNode
from node import Node
import sys
import signal


stop = True;

class ResourceManager(Node):

    # list of jobs that waiting in RM before assigned to node
    job_queue = []

    # id node locally, load per node
    nodes_stat = []

    # id node, jobs on that node. Assumption, 1 node 1 job, 1 job can be > 1 node
    jobs_assigned_node = [(1, "jobobj"), (2, "jobA-obj")]

    def __init__(self, oid, name="RM", nodeamount=Constant.TOTAL_NODE_EACH):
        Node.__init__(self, oid, name)
        print 'rm %s created with id %d' %(name, oid)
        self.nodes_stat = []

        self._creating_wnodes(nodeamount)


    #Activity : add the incoming jobs to the local queue if all the nodes are occupied. Then wait and monitor the system
    def add_job(self,d_job):
        job = serpent.loads(d_job)

        self.job_queue.append(job)

        nodetosubmit = self._choose_nodes()
        self._assignjob(nodetosubmit, job)
        return True

    # receive report from node
    def receivereport(self, details, d_report):
        return 'what?'

    def get_cluster_info(self):
        buff = ""
        for (nid, n) in self.nodes_stat:
            buff += str(n) + "\n"
        return buff

    # assign job to node
    def _assignjob(self, wnode, job):
        print str(wnode)+' assigned job '+ str(job)
        wnode.startjob(job)
        return wnode


    ###Activity : this function takes out the high prioirity job for the RM Queue
    ## output : the latest job
    def _choose_job(self):
        return job_queue.pop()

    # choose nodes available
    def _choose_nodes(self):
        for (nid, n) in self.nodes_stat:
            if n.getload() < 0.7:
                return n

        return None

    # monitor the cluster status
    def _get_cluster_stat(self):
        return None

    # monitor the cluster status
    def _creating_wnodes(self, n):
        for x in range(0,n):
            n_oid = self.oid * 10000 + x
            node = WorkerNode(n_oid, "[WK-"+str(n_oid)+"]")
            self.nodes_stat.append( (n_oid, node) )

    # Activity :  this function report the node details and job completion /failure details to the parent GS
    #
    #output : datastructure with values such as failed node id , avaiable jobs spots, job completion details.
    def _report_toGS(self):
        return True

def check_stop():
    return stop

def main():

    ns = Pyro4.locateNS()

    if len(sys.argv) == 0:
        oid = len(ns.list(prefix=Constant.NAMESPACE_RM+"."))
    else:
        oid = int(sys.argv[1])

    oid = len(ns.list(prefix=Constant.NAMESPACE_RM+"."))

    node = ResourceManager(oid, "[RM-"+str(oid)+"]", Constant.TOTAL_NODE_EACH)

    daemon = Pyro4.Daemon()
    uri = daemon.register(node)
    node.seturi(uri)

    ns.register(Constant.NAMESPACE_RM+"."+node.getname()+str(oid), uri)

    def signal_handler(signal, frame):
        print('You pressed Ctrl+C on RM!')
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
