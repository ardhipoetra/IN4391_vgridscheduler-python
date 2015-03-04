import Pyro4
import sys
from gridscheduler import GridScheduler
from constant import Constant
import utils

if sys.version_info < (3, 0):
    input = raw_input

def main():
	g_sch = GridScheduler()

	gslist = []
	rmlist = []

	count = 0

	for x in xrange(0,Constant.TOTAL_GS):		
		n = utils.add_node(x, "", Constant.NODE_GRIDSCHEDULER)
		gslist.append(n)
		count+=1

	for x in xrange(count, Constant.TOTAL_RM+count):		
		n = utils.add_node(x, gslist[0], Constant.NODE_RESOURCEMANAGER) #for now all RM connected to GS[0]
		rmlist.append(n)	

	out = True
	
	while(out):
	    print "\n\nPlease select: "
	    print "1 Msg GS -> RM"
	    print "other OUT"
	    
	    ip = input("Input:")

	    if ip == '0':
	    	out = False
	    else:
	    	g_sch.submitjob(ip)

if __name__=="__main__":
    main()