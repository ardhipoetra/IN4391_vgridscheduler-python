from __future__ import print_function
import Pyro4
import threading
from node import Node
from constant import Constant
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

    f = open(nameid+".txt","a")

    print(toprint+os.linesep, file=f)
    # print toprint

    f.close()
