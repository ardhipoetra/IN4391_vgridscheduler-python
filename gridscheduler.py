import Pyro4
import threading
from node import Node
from resourcemanager import ResourceManager
from constant import Constant

class DistributedGridScheduler(Node):
    def __init__(self, oid, name="GS"):
        Node.__init__(self, oid, name)
        print 'gs %s created with id %d' %(name, oid)

        self.rmlist = [] #rm connected in this

    def receivereport(self, details, report):
        detobj = Pyro4.Proxy(details)
        print '%s received report %s from %s' %(self,report,detobj.tostr())

    def assignjob(self, assignee, job):
        print '%s assigned job %s' % (self,job)

        ns = Pyro4.locateNS()

        for rm, rm_uri in ns.list(prefix=Constant.NAMESPACE_RM+".").items():
            rmobj = Pyro4.Proxy(rm_uri)
            if rmobj.getoid() == self.chooseRM():
                print 'send job to %s' % (rmobj.tostr())
                rmobj.assignjob(assignee,job)

    def chooseRM(self):

        return Constant.TOTAL_GS

class GridScheduler(object):

    def gslength(self):
        return len(self.gslist)

    def submitjob(self, job):
        ns = Pyro4.locateNS()

        print 'push job %s' %job

        for gs, gs_uri in ns.list(prefix=Constant.NAMESPACE_GS+".").items():
            gsobj = Pyro4.Proxy(gs_uri)
            if gsobj.getoid() == self.chooseGS():
                gsobj.assignjob(gs_uri, job)

    def __init__(self):
        print 'init GS cluster'
        self.gslist = []

    def chooseGS(self):

        return 0
