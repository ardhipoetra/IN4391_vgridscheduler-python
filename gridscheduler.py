import Pyro4
import threading
from node import Node
from resmanager import ResourceManager

class DistributedGridScheduler(Node):
    def __init__(self, oid, name="GS"):
        Node.__init__(self, oid, name)
        print 'gs %s created with id %d' %(name, oid)

        self.rmlist = []

    def addnewRM(self, namespace):

        def _addRM(oid, namespace, resmgr):
            daemon=Pyro4.Daemon()
            rm_uri = daemon.register(resmgr)
            resmgr.seturi(rm_uri)

            ns = Pyro4.locateNS()
            ns.register(namespace+".RM-"+str(oid), rm_uri)
            daemon.requestLoop()

        oid = len(self.rmlist) + 10000
        resmgr=ResourceManager(oid, "RM-"+str(oid)) 
        self.rmlist.append(resmgr)

        thread = threading.Thread(target=_addRM, args=(oid, namespace, resmgr, ))
        thread.setDaemon(True)
        thread.start()

        return resmgr

    def receivereport(self, details, report):
        detobj = Pyro4.Proxy(details)
        print '%s received report %s from %s' %(self,report,detobj.tostr())

    def assignjob(self, assignee, job):
        print '%s assigned job %s' % (self,job)

        ns = Pyro4.locateNS()

        for rm, rm_uri in ns.list(prefix="vgs.resmgr.").items():
            rmobj = Pyro4.Proxy(rm_uri)
            print 'send job to %s' % (rmobj.tostr())
            rmobj.assignjob(assignee,job)


class GridScheduler(object):

    def addnewGS(self, namespace):

        def _addGS(oid, namespace, gsobj):
            daemon=Pyro4.Daemon()
            gs_uri = daemon.register(gsobj)
            gsobj.seturi(gs_uri)

            ns = Pyro4.locateNS()
            ns.register(namespace+"."+gsobj.getname(), gs_uri)
            daemon.requestLoop()

        #creating GS
        oid = len(self.gslist)
        gsname = "GS-%d" %oid
        gs = DistributedGridScheduler(oid, gsname)
        self.gslist.append(gs)

        thread = threading.Thread(target=_addGS, args=(oid, namespace, gs, ))
        thread.setDaemon(True)
        thread.start()
        
        return gs

    def gslength(self):
        return len(self.gslist)

    def submitjob(self, job):
        ns = Pyro4.locateNS()

        print 'push job %s' %job

        for gs, gs_uri in ns.list(prefix="vgs.gridscheduler.").items():
            gsobj = Pyro4.Proxy(gs_uri)
            if gsobj.getid() == 0:
                gsobj.assignjob(gs_uri, job)

    def __init__(self):
        print 'init GS cluster'
        self.gslist = []
