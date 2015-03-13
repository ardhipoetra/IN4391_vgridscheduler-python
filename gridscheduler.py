import Pyro4
import threading
import serpent
from node import Node
from resourcemanager import ResourceManager
from constant import Constant
import Queue

class DistributedGridScheduler(Node):

   ## Define the data structure which maintains the state of each distributed GS
  
   ## Sulabh >>>structure for the JOb details such as GS number it is working on and RM it is assigned to, job number, cluster name
   ## may be a new class to club all these stuff together ? or within the same ???

#-------------------------->>>>>
###define the  structure of the RM and Cluster level data at each GS the data member include will be Cluster number, RM name, Jobs tracking to that cluster.
    def __init__(self, oid, name="GS"):
        Node.__init__(self, oid, name)
        print 'gs %s created with id %d' %(name, oid)

        self.rmlist = [] #rm connected in this

    def receivereport(self, details, d_report):
        detobj = Pyro4.Proxy(details)
        report = serpent.loads(d_report)

        print '%s received report %s from %s' %(self,report,detobj.tostr())

    def assignjob(self, assignee, d_job):
        job = serpent.loads(d_job)

        print '%s assigned job %s' % (self,job)

        ns = Pyro4.locateNS()

        for rm, rm_uri in ns.list(prefix=Constant.NAMESPACE_RM+".").items():
            rmobj = Pyro4.Proxy(rm_uri)
            if rmobj.getoid() == self.chooseRM():
                print 'send job to %s' % (rmobj.tostr())
                rmobj.assignjob(assignee,d_job)

    def chooseRM(self):

      return Constant.TOTAL_GS

    #activity : Inform about the RM who has started executing the job
    ## output : return the sucess/failure status of the update to the calling function
    def update_jobdetailsRM(self):
      return True

    #activity :Update the data structure for consistency/replication for every distributed GS
    def update_GSstructure(self):
      return False

    # # activity : update the distribued GS status to the master node. the details include availability and load, job failure details , GS failure details 
    def sendDgsDetailsToMasterGS(self):
      return true  

class GridScheduler(object):

    def gslength(self):
        return len(self.gslist)


    # I think this function serve the same purpose with submitjob
    # def sendJobDetailsToDistributedGS(self):
    #     return True
    # Sulabh--> we still need this function once the gs is choosen we have to inform the other gs about the this job and which RM it is working on .
    def sendJobDetailsToDistributedGS(self):
      return false    

    def submitjob(self, job, queue):
        ns = Pyro4.locateNS()

        print 'push job %s' %job
        self.maintain_jobqueue(queue,job)
         


        for gs, gs_uri in ns.list(prefix=Constant.NAMESPACE_GS+".").items():
            gsobj = Pyro4.Proxy(gs_uri)
            dgs_object = self.chooseGS()

            if gsobj.getoid() == dgs_object:
                job = self.getJobFromQueue(queue)
                gsobj.assignjob(gs_uri, serpent.dumps(job, indent=True))

    def __init__(self):
      print 'init GS cluster'
      self.gslist = []

    ## activity : based on the availibility and the load choose the right distributed GS
    ##  return : return the selected GS 
    def chooseGS(self):
      return 0

    # activity :Maintain the job queue in case all the Distributed GS are occupiedd
    #output nothing 
    def maintain_jobqueue(self,queue,job):
      queue.put(job)    

    

    ## activity : Function to get the  latest job from the queue.   
    ## output : the num recent job
    def getJobFromQueue(self,queue):

      return queue.get() 

    # get the current status of the cluster 
    # 
    # activity: return the data structure containing the deails such as how many nodes are avaiable, current workload in the network etc
    # output: data structure for the cluster status from the perticular cluster 
    def getCurrentClusterStatus(self, rmobject):
      return true     

