import Pyro4
import sys
import time
from constant import Constant
from constant import Pool
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

adding_gen = False

def main():
    adding_gen = False
    # Pyro4.config.SERIALIZER = "json"
    # Pyro4.config.COMMTIMEOUT=0.5
    os.environ["PYRO_LOGFILE"] = "pyro.log"
    os.environ["PYRO_LOGLEVEL"] = "DEBUG"
    os.environ["THREADPOOL_SIZE"] = "50000"
    # os.environ["SERVERTYPE"] = "multiplex"

    for ip in Pool.POTENTIAL_LINK:
        for i in range(0,Constant.TOTAL_GS):
            try:
                Pyro4.resolve("PYRONAME:%s.[GS-%d]%d@%s" %(Constant.NAMESPACE_GS,i,i,ip))
                if "gs-"+str(i) not in Pool.lookuptable:
                    Pool.lookuptable["gs-"+str(i)] = ip
            except Pyro4.errors.NamingError:
                pass
        for j in range(0,Constant.TOTAL_RM):
            try:
                Pyro4.resolve("PYRONAME:%s.[RM-%d]%d@%s" %(Constant.NAMESPACE_RM,j,j,ip))
                if "rm-"+str(j) not in Pool.lookuptable:
                    Pool.lookuptable["rm-"+str(j)] = ip
            except Pyro4.errors.NamingError:
                pass

    def _jobgen(count):
        for jid in range(0,count):
            time.sleep(random.randint(5,10) * 0.01)
            while True:
                target = random.randint(0, Constant.TOTAL_GS-1)
                try :
                    struri = utils.find(Constant.NODE_GRIDSCHEDULER, target)
                    if struri is None:
                        raise Exception('None in Pool')

                    # tes connection
                    Pyro4.resolve(struri)
                    break
                except Exception as e:
                    print ("GS %d unavailable, try again" %target)
                    continue

            with Pyro4.Proxy(struri) as gsobj:
                jobsu = Job(jid, "gen-jobs-"+str(jid), random.randint(10,35), random.random(), target, time.time())
                d_job = serpent.dumps(jobsu)
                gsobj.addjob(d_job)


        adding_gen = False
        return


    #


    out = True
    count = 0
    while(out):
        print ("\n\nPlease select")
        print ("other input: Add job manually")
        print ("0: OUT")
        print ("1: Add auto jobs")


        ip = input("Input:")
        if ip == '0':
            out = False
        elif ip == '1':
            adding_gen = True
            thread = threading.Thread(target=_jobgen, args=[Constant.NUMBER_OF_JOBS])
            thread.setDaemon(True)
            thread.start()
            print ("%d job try to be submitted" %Constant.NUMBER_OF_JOBS)
            # for rm, rm_uri in nsrm.list(prefix=Constant.NAMESPACE_RM+".").items():
            #     rmobj = Pyro4.Proxy(rm_uri)
            #     print ("from rm : "+str(rmobj.getoid())+" -> "+str(rmobj.get_workloadRM()))
            #     print (rmobj.get_cluster_info())
            #     print ("========\n")
            #     print (rmobj.get_job_node())
            #     print ("----------------\n")
            pass
        elif ip == '2': # see message to send from all GS
            # for gs, gs_uri in nsgs.list(prefix=Constant.NAMESPACE_GS+".").items():
            #     gsobj = Pyro4.Proxy(gs_uri)
            #     print ("from gs : "+str(gsobj.getoid()))
            #     print (gsobj.get_gs_info())
            #     print (";;;;;;;;;;;;;;;;;;;;;;;;;;;;;;\n")
            pass
        elif ip == '3':
            pass
        elif ip == '4': #see status all GS
            # for gs, gs_uri in nsgs.list(prefix=Constant.NAMESPACE_GS+".").items():
            #     gsobj = Pyro4.Proxy(gs_uri)
            #     print ("from gs : "+str(gsobj.getoid()))
            #     print (gsobj.get_all_gs_info())
            #     print (";;;;;;;;;;;;;;;;;;;;;;;;;;;;;;\n")
            pass
        else:
            if adding_gen:
                print ("cant add job, pending job still in process")
                continue

            target = random.randint(0, Constant.TOTAL_GS-1)
            struri = utils.find(Constant.NODE_GRIDSCHEDULER, target)

            jobsu = Job(count, ip+str(count), random.randint(15,25), random.random(), target)
            d_job = serpent.dumps(jobsu)

            with Pyro4.Proxy(struri) as gsobj:
                gsobj.addjob(d_job)
            count+=1
    return

if __name__=="__main__":
    main()
