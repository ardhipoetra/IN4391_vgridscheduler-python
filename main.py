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


    ns = Pyro4.locateNS()

    for gs_i in range(0, Constant.TOTAL_GS):
        subp_gs.append(subprocess.Popen(['python', 'gridscheduler.py', str(gs_i)]))

    for rm_i in range(0, Constant.TOTAL_RM):
        subp_rm.append(subprocess.Popen(['python', 'resourcemanager.py', str(rm_i)]))


    out = True
    count = 0
    while(out):
        print "\n\nPlease select: "
        print "other input: Msg GS -> RM"
        print "0: OUT"

        ip = input("Input:")

        if ip == '0':
            for gi in range(0, Constant.TOTAL_GS):
                os.kill(subp_gs[gi].pid, signal.SIGINT)
            for rmi in range(0, Constant.TOTAL_RM):
                os.kill(subp_rm[rmi].pid, signal.SIGINT)
            out = False
        elif ip == '1':
            for rm, rm_uri in ns.list(prefix=Constant.NAMESPACE_RM+".").items():
                rmobj = Pyro4.Proxy(rm_uri)
                print rmobj.get_cluster_info()
                print "========\n"
        elif ip == '2':
            pass
        elif ip == '3':
            pass
        else:
            # def _newjob(count):
            uri = ns.lookup(Constant.NAMESPACE_GS+"."+"[GS-0]0")
            gsobj = Pyro4.Proxy(uri)

            jobsu = Job(count, "joob"+str(count), random.randint(5,15), random.random())
            d_job = serpent.dumps(jobsu)

            gsobj.addjob(d_job)

            # thread = threading.Thread(target=_newjob, args=[count])
            # thread.setDaemon(True)
            # thread.start()

    return

if __name__=="__main__":
    main()
