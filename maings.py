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

def maings():

    # Pyro4.config.SERIALIZER = "json"
    # Pyro4.config.COMMTIMEOUT=0.5
    os.environ["PYRO_LOGFILE"] = "pyro.log"
    os.environ["PYRO_LOGLEVEL"] = "DEBUG"
    os.environ["THREADPOOL_SIZE"] = "50000"
    # os.environ["SERVERTYPE"] = "multiplex"

    ns = Pyro4.locateNS(host=Constant.IP_GS_NS)

    for gs_i in range(0, Constant.TOTAL_GS):
        subp_gs.append(subprocess.Popen(['python', 'gridscheduler.py', str(gs_i)]))

    # for rm_i in range(0, Constant.TOTAL_RM):
    #     subp_rm.append(subprocess.Popen(['python', 'resourcemanager.py', str(rm_i)]))


    out = True
    count = 0
    while(out):
        print ("\n\nPlease select")
        print ("other input: Msg GS -> RM")
        print ("0: OUT")

        ip = input("Input:")

        if ip == '0':
            for gi in range(0, Constant.TOTAL_GS):
                os.kill(subp_gs[gi].pid, signal.SIGINT)
            out = False
        elif ip == '1':
            for gs, gs_uri in ns.list(prefix=Constant.NAMESPACE_GS+".").items():
                with Pyro4.Proxy(gs_uri) as gsobj:
                    print ("from gs : "+str(gsobj.getoid()))
                    print (gsobj.get_gs_info())
                    print (";;;;;;;;;;;;;;;;;;;;;;;;;;;;;;\n")
        elif ip.startswith("killgs"):
            keykill, idkill_s = ip.split()
            idkill = int(idkill_s)
            os.kill(subp_gs[idkill].pid, signal.SIGINT)
        elif ip.startswith("spawngs"):
            keyspw, idspw_s = ip.split()
            idspw = int(idspw_s)
            subp_gs[idspw] = subprocess.Popen(['python', 'gridscheduler.py', str(idspw)])
        else:
            pass

    return

if __name__=="__main__":
    maings()
