import Pyro4
import sys
import time
# from gridscheduler import GridScheduler
from constant import Constant
from job import Job
import utils
import serpent
import os
import subprocess
import random
import signal
import threading

if sys.version_info < (3, 0):
    input = raw_input

subp_gs = []
subp_rm = []

def mainrm():

    # Pyro4.config.SERIALIZER = "json"
    # Pyro4.config.COMMTIMEOUT=0.5
    os.environ["PYRO_LOGFILE"] = "pyro.log"
    os.environ["PYRO_LOGLEVEL"] = "DEBUG"
    Pyro4.config.THREADPOOL_SIZE = 50000

    ns = Pyro4.locateNS(host=Constant.IP_RM_NS)

    for rm_i in range(0, Constant.TOTAL_RM):
        subp_rm.append(subprocess.Popen(['python', 'resourcemanager.py', str(rm_i)]))

    out = True
    count = 0
    while(out):
        print ("\n\nPlease select")
        print ("other input: Msg GS -> RM")
        print ("0: OUT")

        ip = input("Input:")

        if ip == '0':
            for rmi in range(0, Constant.TOTAL_RM):
                os.kill(subp_rm[rmi].pid, signal.SIGINT)
            out = False
        elif ip == '1':
            for rm, rm_uri in ns.list(prefix=Constant.NAMESPACE_RM+".").items():
                rmobj = Pyro4.Proxy(rm_uri)
                print ("from rm : "+str(rmobj.getoid())+" -> "+str(rmobj.get_workloadRM()))
                print ("queue => "+str(rmobj.get_jobq()))
                print (rmobj.get_cluster_info())
                print ("========\n")
        elif ip == '2':
            for rm, rm_uri in ns.list(prefix=Constant.NAMESPACE_RM+".").items():
                rmobj = Pyro4.Proxy(rm_uri)
                print ("from rm : "+str(rmobj.getoid())+" -> "+str(rmobj.get_workloadRM()))
                print (rmobj.get_job_node())
                print ("----------------\n")
        elif ip.startswith("killrm"):
            keykill, idkill_s = ip.split()
            idkill = int(idkill_s)
            os.kill(subp_rm[idkill].pid, signal.SIGINT)
        elif ip.startswith("spawnrm"):
            keyspw, idspw_s = ip.split()
            idspw = int(idspw_s)
            subp_rm[idspw] = subprocess.Popen(['python', 'resourcemanager.py', str(idspw)])
        else:
            pass

    return

if __name__=="__main__":
    mainrm()
