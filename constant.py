
class Constant(object):
	# IP_GS_NS = "10.149.3.6"
	# IP_RM_NS = "10.149.3.11"

	TOTAL_GS = 3
	TOTAL_RM = 5
	TOTAL_NODE_EACH = 20
	TOTAL_NODE = TOTAL_RM * TOTAL_NODE_EACH

	NUMBER_OF_JOBS = 100

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
		"10.149.3.6",
		"10.149.3.11"
	]

	lookuptable = dict()
