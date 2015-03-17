import Pyro4
import serpent
from node import Node
import sys

class ResourceManager(Node):

    # list of jobs that waiting in RM before assigned to node
    job_queue = []

    # id node locally, load per node
    nodes_loads = [(1,0.8), (2, 0.4)]

    # id node, jobs on that node. Assumption, 1 node 1 job, 1 job can be > 1 node
    jobs_assigned_node = [(1, "jobobj"), (2, "jobA-obj")]

    def __init__(self, oid, name="RM", nodeamount=Constant.TOTAL_NODE_EACH):
        Node.__init__(self, oid, name)
        print 'rm %s created with id %d' %(name, oid)
        self._creating_wnodes(nodeamount)

    #Activity : add the incoming jobs to the local queue if all the nodes are occupied. Then wait and monitor the system
    def add_job(self):
    	return True

    # receive report from node
    def receivereport(self, details, d_report):
        return 'what?'

    # assign job to node
    def _assignjob(self, assignee, d_job):

    	ns = Pyro4.locateNS()
    	asgn = Pyro4.Proxy(assignee)
        job = serpent.loads(d_job)

        print '%s got job %s from %s' %(self, job, asgn.tostr())
    	asgn.receivereport(self.uri, d_job)

    ###Activity : this function takes out the high prioirity job for the RM Queue
    ## output : the latest job
    def _choose_job(self):
    	return "the job"

    # choose nodes available
    def _choose_nodes(self):
        return "list of nodes"

    # monitor the cluster status
    def _get_cluster_stat(self):
        return 'overall cluster status'

    # monitor the cluster status
    def _creating_wnodes(self, n):

        def _add_node(oid):
            ns = Pyro4.locateNS()
            node = WorkerNode(oid, "[WK-"+str(oid)+"]")

            daemon = Pyro4.Daemon()
            uri = daemon.register(node)
            node.seturi(uri)

            ns.register(Constant.NAMESPACE_WK+"."+node.getname()+str(oid), uri)

            daemon.requestLoop()

            return

        for x in range(0,n):
            thread = threading.Thread(target=_add_node, args=(self.oid * 10000 + x))
            thread.setDaemon(True)
            thread.start()

        return 'overall cluster status'

    # Activity :  this function report the node details and job completion /failure details to the parent GS
    #
    #output : datastructure with values such as failed node id , avaiable jobs spots, job completion details.
    def _report_toGS(self):
    	return True

def main():

    ns = Pyro4.locateNS()

    if len(sys.argv) == 0:
        oid = len(ns.list(prefix=Constant.NAMESPACE_RM+"."))
    else
        oid = int(sys.argv[0])

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
