from __future__ import print_function
import Pyro4
import threading
from node import Node
from constant import Constant
from constant import Pool
import time
import os


def initarraylist_none(length):
    l = []
    for i in range(0, length):
        l.append([None])
    return l

def write(name, idp, stringtoprint):
    timestamp = "[%f] " %time.time()
    nameid  = "%s-%d" %(name, idp)

    toprint = timestamp + nameid + " | " + stringtoprint

    f = open("logs/"+nameid+".txt","a")

    print(toprint+os.linesep, file=f)
    # print(toprint)

    f.close()

def find(type, nid):
    if type == Constant.NODE_GRIDSCHEDULER:
        header = "gs-"
        namespace = Constant.NAMESPACE_GS
    elif type == Constant.NODE_RESOURCEMANAGER:
        header = "rm-"
        namespace = Constant.NAMESPACE_RM

    key = header+str(nid)
    try:
        ip = Pool.lookuptable[key]
        if ip is None or ip == "":
            return None
    except KeyError:
        return None


    return "PYRONAME:%s.[RM-%d]%d@%s" %(namespace,nid,nid,ip)
