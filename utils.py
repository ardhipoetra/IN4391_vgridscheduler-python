import Pyro4
import threading
from node import Node
from resourcemanager import ResourceManager
from gridscheduler import DistributedGridScheduler
from constant import Constant

def add_node(oid, parent="", type=Constant.NODE_WORKER):

    def _add_node(oid, namespace, parent, node):
        if isinstance(node, ResourceManager):
            print "add RM"
        elif isinstance(node, DistributedGridScheduler):
            print "add DGS"

        daemon = Pyro4.Daemon()
        uri = daemon.register(node)
        node.seturi(uri)

        ns = Pyro4.locateNS()
        ns.register(namespace+"."+node.getname()+str(oid), uri)
        daemon.requestLoop()

    if type == Constant.NODE_WORKER:
        node = WorkerNode(oid, "WK-"+str(oid)) 
        ns = Constant.NAMESPACE_WK
    elif type == Constant.NODE_RESOURCEMANAGER:
        node = ResourceManager(oid, "RM-"+str(oid)) 
        ns = Constant.NAMESPACE_RM
    elif type == Constant.NODE_GRIDSCHEDULER:
        node = DistributedGridScheduler(oid, "GS-"+str(oid)) 
        ns = Constant.NAMESPACE_GS
    else:
        print "ERROR"
        node = ""
        return 

    thread = threading.Thread(target=_add_node, args=(oid, ns, parent, node, ))
    thread.setDaemon(True)
    thread.start()

    return node