
class Constant(object):

	TOTAL_GS = 2
	TOTAL_RM = 3
	TOTAL_NODE = TOTAL_RM * 4


	WORKER_STATUS_IDLE = 0
	WORKER_STATUS_BUSY = 1

	gslist = []
	rmlist = [] # cluster list also?


	NODE_WORKER = "workernode"
	NODE_RESOURCEMANAGER = "RMnode"
	NODE_GRIDSCHEDULER = "GSnode"

	NAMESPACE_RM = "rd.vgs.resourcemanager"
	NAMESPACE_GS = "rd.vgs.gridscheduler"
	NAMESPACE_WK = "rd.vgs.workernode"
