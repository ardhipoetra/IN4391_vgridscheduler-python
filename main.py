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


    nsrm = Pyro4.locateNS(host=Constant.IP_RM_NS)
    nsgs = Pyro4.locateNS(host=Constant.IP_GS_NS)

    def _jobgen(count,gs_ns):
        for jid in range(0,count):
            time.sleep(random.randint(50,200) * 0.01)
            while True:
                target = random.randint(0, Constant.TOTAL_GS-1)
                try :
                    uri = gs_ns.lookup(Constant.NAMESPACE_GS+"."+"[GS-"+str(target)+"]"+str(target))
                    break
                except Pyro4.errors.NamingError as e:
                    print ("GS %d unavailable, try again", %target)
                    continue

            gsobj = Pyro4.Proxy(uri)
            jobsu = Job(jid, "gen-jobs-"+str(jid), random.randint(10,65), random.random(), target)
            d_job = serpent.dumps(jobsu)
            gsobj.addjob(d_job)
        return


    thread = threading.Thread(target=_jobgen, args=[Constant.NUMBER_OF_JOBS,nsgs])
    thread.setDaemon(True)
    thread.start()


    out = True
    count = 0
    while(out):
        print ("\n\nPlease select")
        print ("other input: Msg GS -> RM")
        print ("0: OUT")

        ip = input("Input:")

        if ip == '1':
            for rm, rm_uri in nsrm.list(prefix=Constant.NAMESPACE_RM+".").items():
                rmobj = Pyro4.Proxy(rm_uri)
                print ("from rm : "+str(rmobj.getoid())+" -> "+str(rmobj.get_workloadRM()))
                print (rmobj.get_cluster_info())
                print ("========\n")
                print (rmobj.get_job_node())
                print ("----------------\n")
        elif ip == '2': # see message to send from all GS
            for gs, gs_uri in nsgs.list(prefix=Constant.NAMESPACE_GS+".").items():
                gsobj = Pyro4.Proxy(gs_uri)
                print ("from gs : "+str(gsobj.getoid()))
                print (gsobj.get_gs_info())
                print (";;;;;;;;;;;;;;;;;;;;;;;;;;;;;;\n")
        elif ip == '3':
            pass
        elif ip == '4': #see status all GS
            for gs, gs_uri in nsgs.list(prefix=Constant.NAMESPACE_GS+".").items():
                gsobj = Pyro4.Proxy(gs_uri)
                print ("from gs : "+str(gsobj.getoid()))
                print (gsobj.get_all_gs_info())
                print (";;;;;;;;;;;;;;;;;;;;;;;;;;;;;;\n")
        else:
            pass
            # for now send to GS 0
            # target = random.randint(0, Constant.TOTAL_GS-1)
            # uri = nsgs.lookup(Constant.NAMESPACE_GS+"."+"[GS-"+str(target)+"]"+str(target))
            # gsobj = Pyro4.Proxy(uri)
            #
            # jobsu = Job(count, ip+str(count), random.randint(15,25), random.random(), target)
            #
            # d_job = serpent.dumps(jobsu)
            #
            # gsobj.addjob(d_job)
            # count+=1
    return

if __name__=="__main__":
    main()
