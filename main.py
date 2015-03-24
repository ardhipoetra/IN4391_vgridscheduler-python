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

def main():

    # Pyro4.config.SERIALIZER = "json"
    # Pyro4.config.COMMTIMEOUT=0.5
    os.environ["PYRO_LOGFILE"] = "pyro.log"
    os.environ["PYRO_LOGLEVEL"] = "DEBUG"


    ns = Pyro4.locateNS(host=Constant.IP_NS)

    out = True
    count = 0
    while(out):
        print ("\n\nPlease select")
        print ("other input: Msg GS -> RM")
        print ("0: OUT")

        ip = input("Input:")

    if ip == '1':
            for rm, rm_uri in ns.list(prefix=Constant.NAMESPACE_RM+".").items():
                rmobj = Pyro4.Proxy(rm_uri)
                print ("from rm : "+str(rmobj.getoid())+" -> "+str(rmobj.get_workloadRM()))
                print (rmobj.get_cluster_info())
                print ("========\n")
                print (rmobj.get_job_node())
                print ("----------------\n")
        elif ip == '2': # see message to send from all GS
            for gs, gs_uri in ns.list(prefix=Constant.NAMESPACE_GS+".").items():
                gsobj = Pyro4.Proxy(gs_uri)
                print ("from gs : "+str(gsobj.getoid()))
                print (gsobj.get_gs_info())
                print (";;;;;;;;;;;;;;;;;;;;;;;;;;;;;;\n")
        elif ip == '3':
            pass
        elif ip == '4': #see status all GS
            for gs, gs_uri in ns.list(prefix=Constant.NAMESPACE_GS+".").items():
                gsobj = Pyro4.Proxy(gs_uri)
                print ("from gs : "+str(gsobj.getoid()))
                print (gsobj.get_all_gs_info())
                print (";;;;;;;;;;;;;;;;;;;;;;;;;;;;;;\n")
        else:
            # for now send to GS 0
            target = random.randint(0, Constant.TOTAL_GS-1)
            uri = ns.lookup(Constant.NAMESPACE_GS+"."+"[GS-"+str(target)+"]"+str(target))
            gsobj = Pyro4.Proxy(uri)

            jobsu = Job(count, ip+str(count), random.randint(15,25), random.random(), target)

            d_job = serpent.dumps(jobsu)

            gsobj.addjob(d_job)
            count+=1

            # thread = threading.Thread(target=_newjob, args=[count])
            # thread.setDaemon(True)
            # thread.start()

    return

if __name__=="__main__":
    main()
