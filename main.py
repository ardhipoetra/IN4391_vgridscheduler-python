import Pyro4
import sys
from gridscheduler import GridScheduler
from constant import Constant
from job import Job
import utils
import os

if sys.version_info < (3, 0):
    input = raw_input

def main():

    # Pyro4.config.SERIALIZER = "json"
    Pyro4.config.COMMTIMEOUT=0.5
    os.environ["PYRO_LOGFILE"] = "pyro.log"
    os.environ["PYRO_LOGLEVEL"] = "DEBUG"

    g_sch = GridScheduler()

    gslist = []
    rmlist = []

    count = 0

    for x in xrange(0,Constant.TOTAL_GS):
    	n = utils.add_node(x, "", Constant.NODE_GRIDSCHEDULER)
    	gslist.append(n)
    	count+=1

    for x in xrange(count, Constant.TOTAL_RM+count):
    	n = utils.add_node(x, gslist[0], Constant.NODE_RESOURCEMANAGER) #for now all RM connected to GS[0] FIX ME
    	rmlist.append(n)

    out = True
    count = 0
    while(out):
        # sulabh >> We should ask for the priority of the job here and higher priority jobs go first in the cluster
        print "\n\nPlease select: "
        print "other input: Msg GS -> RM"
        print "0: OUT"

        ip = input("Input:")

        if ip == '0':
        	out = False
        else:
        	j_ip = Job(count, ip, 1000)
        	g_sch.submitjob(j_ip)
        	count+=1

if __name__=="__main__":
    main()
