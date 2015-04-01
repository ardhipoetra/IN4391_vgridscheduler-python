import serpent

class MessageRMGS(object):
    RM_assigned_id = 0
    GS_assigned_id = 1

    job_ids = [0,1,2,3]
    cluster_name = 'cluter alpha'


class MessageGS(object):
    availability_status = "some availability metrics"
    load = 80.1
    jobs_handled = ["job1", "job2"]
