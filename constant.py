
class Constant(object):
	TOTAL_GS = 2
	TOTAL_RM = 3
	TOTAL_NODE_EACH = 20
	TOTAL_NODE = TOTAL_RM * TOTAL_NODE_EACH

	NUMBER_OF_JOBS = 500

	WORKER_STATUS_IDLE = 0
	WORKER_STATUS_BUSY = 1

	NODE_WORKER = "workernode"
	NODE_RESOURCEMANAGER = "RMnode"
	NODE_GRIDSCHEDULER = "GSnode"

	NAMESPACE_RM = "rd.vgs.resourcemanager"
	NAMESPACE_GS = "rd.vgs.gridscheduler"
	NAMESPACE_WK = "rd.vgs.workernode"

class Pool(object):
	POTENTIAL_LINK = [
		"10.141.3.6",
		"10.141.3.11"
	]

	lookuptable = dict()
