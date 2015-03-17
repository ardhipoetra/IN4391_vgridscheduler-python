import Pyro4
import threading
import serpent
from node import Node
from resourcemanager import ResourceManager
from constant import Constant
import utils
import time
import signal
import sys

class GridScheduler(Node):

    # list of jobs that waiting
    job_queue = []

    # id RM, load per RM/cluster
    RM_loads = [(1,0.8), (2, 0.4)]

    # id RM, jobs on that cluster-RM
    jobs_assigned_RM = [(1, ["job1", "job2"]), (2, ["jobA", "jobC"])]

   ## Define the data structure which maintains the state of each GS
    def __init__(self, oid, name="GS"):
        Node.__init__(self, oid, name)
        print 'gs %s created with id %d' %(name, oid)

        self.rmlist = [] #rm connected in this

    # report received after finishing the task from RM
    def receivereport(self, details, d_report):
        detobj = Pyro4.Proxy(details)
        report = serpent.loads(d_report)

        print '%s received report %s from %s' %(self,report,detobj.tostr())

    # assign job to RM
    def assignjob(self, assignee, d_job):
        job = serpent.loads(d_job)

        print '%s assigned job %s' % (self,job)

        ns = Pyro4.locateNS()

        for rm, rm_uri in ns.list(prefix=Constant.NAMESPACE_RM+".").items():
            rmobj = Pyro4.Proxy(rm_uri)
            if rmobj.getoid() == self.chooseRM():
                print 'send job to %s' % (rmobj.tostr())
                rmobj.assignjob(assignee,d_job)

    def choose_job(self):

        return 'job'

    def chooseRM(self):

      return Constant.TOTAL_GS

    #activity : Inform about the RM who has started executing the job
    ## output : return the sucess/failure status of the update to the calling function
    def update_jobdetailsRM(self):
      return True

    #activity :Update the data structure for consistency/replication for designated distributed GS (neighbor)
    def update_GSstructure(self):
      return False

    # push current structure to other GS (consistency)
    def push_structure(self):
        return True

    # handle when retrieving state from other GS
    def get_structure(self, id_GS, structureObj):
        return True;

    # get the current status of the cluster in his jurisdiction
    # activity: return the data structure containing the deails such as how many nodes are avaiable, current workload in the network etc
    # output: data structure for the cluster status from the perticular cluster
    def getclusterstatus(self, rmobject):
        return True

def check_stop():
    return stop
stop = True

def main():
    # g_sch = GridScheduler()
    ns = Pyro4.locateNS()

    oid = len(ns.list(prefix=Constant.NAMESPACE_GS+"."))

    node = DistributedGridScheduler(oid, "[GS-"+str(oid)+"]")

    daemon = Pyro4.Daemon()
    uri = daemon.register(node)
    node.seturi(uri)

    ns.register(Constant.NAMESPACE_GS+"."+node.getname()+str(oid), uri)

    def signal_handler(signal, frame):
        print('You pressed Ctrl+C!')
        stop = False
        daemon.shutdown()


    signal.signal(signal.SIGINT, signal_handler)

    daemon.requestLoop(loopCondition=check_stop)



if __name__=="__main__":
    main()
