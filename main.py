import Pyro4
import sys
from gridscheduler import GridScheduler

if sys.version_info < (3, 0):
    input = raw_input

def main():
	g_sch = GridScheduler()

	for x in xrange(1,3):		
		gs = g_sch.addnewGS("vgs.gridscheduler") #add GS(es)
		rm = gs.addnewRM("vgs.resmgr") #add one RM per GS jurisdiction

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