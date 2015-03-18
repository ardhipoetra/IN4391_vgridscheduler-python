import Pyro4
import threading
from node import Node
from resourcemanager import ResourceManager
from gridscheduler import GridScheduler
from constant import Constant

def add_node(oid, parent="", type=Constant.NODE_WORKER):

    def _add_node(oid, namespace, parent, node):
        if isinstance(node, ResourceManager):
            print "add RM"
        elif isinstance(node, DistributedGridScheduler):
            print "add DGS"

        #deamon should be started with the seperate host IP and port address
        ## which IP address should be taken and how ?
        daemon = Pyro4.Daemon()
        uri = daemon.register(node)
        node.seturi(uri)

        ns = Pyro4.locateNS()
        ns.register(namespace+"."+node.getname()+str(oid), uri)
        daemon.requestLoop()

        return

    if type == Constant.NODE_WORKER:
        anode = WorkerNode(oid, "[WK-"+str(oid)+"]")
        ns = Constant.NAMESPACE_WK
    elif type == Constant.NODE_RESOURCEMANAGER:
        node = ResourceManager(oid, "[RM-"+str(oid)+"]")
        ns = Constant.NAMESPACE_RM
    elif type == Constant.NODE_GRIDSCHEDULER:
        node = DistributedGridScheduler(oid, "[GS-"+str(oid)+"]")
        ns = Constant.NAMESPACE_GS
    else:
        print "ERROR"
        node = ""
        return

     # Should be started as a seperate process separaate memory
    thread = threading.Thread(target=_add_node, args=(oid, ns, parent, node, ))
    thread.setDaemon(True)
    thread.start()

    return node
