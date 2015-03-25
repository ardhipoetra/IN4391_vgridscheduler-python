import Pyro4
import threading
import serpent
from node import Node
from resourcemanager import ResourceManager
from constant import Constant
import utils
import time
import signal
import sys
from operator import attrgetter
import random

stop = True

class GridScheduler(Node):

    # list of jobs that waiting
    job_queue = []

    # id RM, load per RM/cluster
    RM_loads = [0.0] * Constant.TOTAL_RM
    #RM_loads = []

    # id RM, jobs on that cluster-RM
    jobs_assigned_RM = []

    # every GS store everyone's state, except himself, maybe. including timestamp
    neighbor_stateGS = [None] * Constant.TOTAL_GS

    message_GS = {
        'id' : -1,
        'timestamp' : 1234567,
        'job_queue' : job_queue,
        'RM_loads' : RM_loads,
        'jobs_assigned_RM' : jobs_assigned_RM
    }

    def _write(self, string):
        utils.write(Constant.NODE_GRIDSCHEDULER, self.oid, string)

    def _periodicresult(self):
        while True:
            time.sleep(3)
            self._write("[CLUSTERLOAD]"+self.get_csvrmload())

   ## Define the data structure which maintains the state of each GS
    def __init__(self, oid, name="GS"):
        Node.__init__(self, oid, name)
        utils.write(Constant.NODE_RESOURCEMANAGER, self.oid, "created with id %d" %(oid))

        self.jobs_assigned_RM = utils.initarraylist_none(Constant.TOTAL_RM)
        self.RM_loads = [0.0 for i in range(Constant.TOTAL_RM)] #rm connected in this


        thread = threading.Thread(target=self._periodicresult)
        thread.setDaemon(True)
        thread.start()

    # report received after finishing the task from RM
    @Pyro4.oneway
    def receive_report(self, rmid, d_job):
        job = serpent.loads(d_job)
        self._write('received report %s from RM-%d' %(job, rmid))

        # remove from stat monitor
        self.RM_loads[rmid] -= job["load"]
        self.jobs_assigned_RM[rmid].remove(job)

        # resync with neighbor
        (act_neig, inact_neig) = self._monitorneighborGS()
        # monitor RM
        self._monitorRM()

        if len(inact_neig) != 0: #there some inactive GS
            self._takeover_jobs(inact_neig)

        self._push_structure(act_neig, self._update_GSstructure())

        timedone = time.time()
        tdiff = timedone - job["starttime"]
        self._write('job %s GS-FINISHED at %f (%f)' %(job, timedone, tdiff))

        # if there's job in the queue
        # if len(self.job_queue) != 0:
        #     self._write("queue not empty, try assign job to RM")
        #     rmidsub = self._chooseRM()
        #     if rmidsub == -1:
        #         self._write('no rm available!')
        #         return False
        #     else:
        #         jobsub = self._choose_job()
        #
        #         if jobsub is None:
        #             self._write("no job available")
        #         else:
        #             jobsub["RM_assigned"] = rmidsub
        #             self._assignjob(rmidsub, serpent.dumps(jobsub))

    # add job to this GS
    @Pyro4.oneway
    def addjob(self, d_job):
        job = serpent.loads(d_job)
        self._write("get job {%s} added" %(job))
        self.job_queue.append(job)


        self._monitorRM()
        rmidsub = self._chooseRM()

        if rmidsub == -1 :
            self._write('no rm available!')
        else:
            jobsub = self._choose_job()

            if jobsub is None:
                self._write("no job available")
            else:
                jobsub["RM_assigned"] = rmidsub
                self._assignjob(rmidsub, serpent.dumps(jobsub))

    # handle when retrieving state from other GS
    def get_structure(self, id_GS, d_structureObj):
        msg_gs = serpent.loads(d_structureObj)

        id_source = int(msg_gs["id"])
        return_stat = 0;
        if self.neighbor_stateGS[id_source] is None:
            self.neighbor_stateGS[id_source] = (msg_gs["timestamp"], msg_gs)
            return_stat = 1
        else:
            (ts,msg_avail) = self.neighbor_stateGS[id_source]
            if ts < msg_gs["timestamp"]:
                self.neighbor_stateGS[id_source] = (msg_gs["timestamp"], msg_gs)
                return_stat = 2
            else:
                return_stat = 3
                pass

        return return_stat;

    def get_csvrmload(self):
        buff = ""
        for idx, jobl in enumerate(self.jobs_assigned_RM):
            buff += "\t%f;%d," %(self.RM_loads[idx], len(jobl)-1)
        return buff


    def get_gs_info(self):
        buff = ""
        buff += "ID : "+str(self.oid) + "\n"
        buff += "job_queue : "+str(self.job_queue) + "\n"
        buff += "RM_loads : "+str(self.RM_loads) + "\n"
        buff += "jobs_assigned_RM : \n"
        for jobl in self.jobs_assigned_RM:
            buff += "\t["+str(len(jobl)-1)+"] - "
            for ajob in jobl:
                if ajob is not None:
                    buff += str(ajob["jid"])+","
            buff += "\n"

        buff += "------------------------------------------------\n"
        return buff

    def get_all_gs_info(self):
        buff = ""
        for idx, neighbor_stats in enumerate(self.neighbor_stateGS):
            if neighbor_stats is not None:
                (ts, ns) = neighbor_stats

                buff += "ID : "+str(ns["id"]) + "\n"
                buff += "job_queue : "+str(ns["job_queue"]) + "\n"
                buff += "RM_loads : "+str(ns["RM_loads"]) + "\n"
                buff += "jobs_assigned_RM :"+str(ns["jobs_assigned_RM"]) + "\n"
                buff += "<><><><>><><><><><><><>"
        return buff

    # assign job to RM
    @Pyro4.oneway
    def _assignjob(self, rmid, d_job):
        job = serpent.loads(d_job)

        ns = Pyro4.locateNS(host=Constant.IP_RM_NS)
        uri = ns.lookup(Constant.NAMESPACE_RM+"."+"[RM-"+str(rmid)+"]"+str(rmid))

        with Pyro4.Proxy(uri) as rmobj:
            self._write('send job to %s' % (rmobj.tostr()))

            self.RM_loads[rmid] += job["load"]

            self.jobs_assigned_RM[rmid].append(job)

            (act_neig, inact_neig) = self._monitorneighborGS()

            if len(inact_neig) != 0: #there some inactive GS
                self._takeover_jobs(inact_neig)

            self._push_structure(act_neig, self._update_GSstructure())

            # aycall = Pyro4.async(rmobj)
            rmobj.add_job(d_job)

    # GS choose job
    def _choose_job(self):
        try:
            job = self.job_queue.pop()
            self._write("choose job to submit..%s" %job)
            return job
        except IndexError:
            return None

    def _chooseRM(self):
        ns = Pyro4.locateNS(host=Constant.IP_RM_NS)
        rm_tmp = [0.0] * Constant.TOTAL_RM
        for rm, rm_uri in ns.list(prefix=Constant.NAMESPACE_RM+".").items():
            with Pyro4.Proxy(rm_uri) as rmobj:
                rm_tmp[rmobj.getoid()] = rmobj.get_workloadRM()

        x = sorted(rm_tmp)

        if x[0] < 0.9 and x[0] >= 0.0:
            self._write("found RM to submit with "+str(x[0]))
            return rm_tmp.index(x[0])
        else:
            self._write("no RM available")
            return -1

    # Inform about the RM who has started executing the job
    def _update_jobdetailsRM(self): # to be honest I don't understand this function
        return True

    # Update the data structure for consistency/replication to designated distributed GS (neighbor) -> create snapshot
    def update_GSstructure(self):
        return self._update_GSstructure()

    def _update_GSstructure(self):
        self.message_GS = {
            'id' : self.oid,
            'timestamp' : time.time(),
            'job_queue' : self.job_queue,
            'RM_loads' : self.RM_loads,
            'jobs_assigned_RM' : self.jobs_assigned_RM
        }

        return serpent.dumps(self.message_GS)

    # push current structure to other GS (consistency)
    # assumed gs_listid is active GS, msg is serpent dumps
    def _push_structure(self, gs_listid, msg_gs):

        self._write("push state to neighbor")

        ns = Pyro4.locateNS(host=Constant.IP_GS_NS)

        for gsid in gs_listid:
            gso_uri = ns.lookup(Constant.NAMESPACE_GS+"."+"[GS-"+str(gsid)+"]"+str(gsid))

            with Pyro4.Proxy(gso_uri) as gsobj :
                gsobj.get_structure(self.oid, msg_gs)

        return True

    # monitor GS to handle fault in GS
    def _monitorneighborGS(self):
        ns = Pyro4.locateNS(host=Constant.IP_GS_NS)

        activeid = [self.oid]
        inactiveid = []
        for gs_o, gso_uri in ns.list(prefix=Constant.NAMESPACE_GS+".").items():
            with Pyro4.Proxy(gso_uri) as gsobj :
                if gsobj.getoid() != self.oid:
                    activeid.append(gsobj.getoid())

        if len(activeid) != Constant.TOTAL_GS:
            inactiveid = list(set([x for x in range(0, Constant.TOTAL_GS)]) - set(activeid))

        self._write("active GS : %s | inactive GS : %s" %(str(activeid), str(inactiveid)))

        return (activeid, inactiveid)

    # monitor RM to handle fault in RM
    def _monitorRM(self):
        ns = Pyro4.locateNS(host=Constant.IP_RM_NS)

        # only look at related RM
        for rmid, jobs_in_rm in enumerate(self.jobs_assigned_RM):
            if jobs_in_rm == [None]:
                continue

            try:
                self._write("monitoring check RM %d" %(rmid))
                ns.lookup(Constant.NAMESPACE_RM+"."+"[RM-"+str(rmid)+"]"+str(rmid))
            except Exception as e:
                # handle dead RM
                self._write("RM %d detected dead!" %(rmid))

                # clean stats
                self.jobs_assigned_RM[rmid] = [None]

                for jobs in jobs_in_rm:
                    if jobs is None:
                        continue

                    # readd this job to queue
                    jobs["RM_assigned"] = -1
                    self._write("readd job "+str(jobs))
                    self.addjob(serpent.dumps(jobs))

    def _takeover_jobs(self, inactive_lid):
        takeover_gsid=[]

        # agreement of GS(es)
        for gsdown_id in sorted(inactive_lid):
            # if already stated as dead, do nothing
            if self.neighbor_stateGS[gsdown_id] is None:
                self._write("not take care %d [NONE VAL]" %gsdown_id)
                continue

            if self.oid == gsdown_id - 1: #just handled by gs before it
                takeover_gsid.append(gsdown_id)
            elif gsdown_id - 1 < 0 and self.oid == Constant.TOTAL_GS - 1: # if 0 died
                takeover_gsid.append(gsdown_id)
            pass

            if len(takeover_gsid) != 0:
                if max(takeover_gsid) + 1 == gsdown_id: #consecutive Gs down
                    takeover_gsid.append(gsdown_id)


        for tgsid in takeover_gsid:
            #handle job if needed
            (ts, gs_state) = self.neighbor_stateGS[tgsid]

            #set neighbor stat as died
            self._write("take care GS dead:%d" %tgsid)
            self.neighbor_stateGS[tgsid] = None

            # take over the job already run in RM, thus will discard it
            for ljob_inrm in gs_state["jobs_assigned_RM"]:
                for job_in_rm in ljob_inrm:
                    if job_in_rm is not None:
                        job_in_rm["GS_assignee"] = self.oid
                        job_in_rm["RM_assigned"] = -1
                        self._write("re-add jobs %d in GS %d" %(job_in_rm["jid"], tgsid))
                        self.addjob(serpent.dumps(job_in_rm))

            # also add the job in its queue
            for dj in gs_state["job_queue"]:
                dj["GS_assignee"] = self.oid
                self.addjob(dj)

def check_stop():
    return stop

def main():
    # g_sch = GridScheduler()
    ns = Pyro4.locateNS(host=Constant.IP_GS_NS)

    if len(sys.argv) == 0:
        oid = len(ns.list(prefix=Constant.NAMESPACE_GS+"."))
    else:
        oid = int(sys.argv[1])

    node = GridScheduler(oid, "[GS-"+str(oid)+"]")

    daemon = Pyro4.Daemon(Constant.IP_GS_NS)
    uri = daemon.register(node)
    node.seturi(uri)

    ns.register(Constant.NAMESPACE_GS+"."+node.getname()+str(oid), uri)

    def signal_handler(signal, frame):
        utils.write(Constant.NODE_GRIDSCHEDULER, oid, "GS will down!")
        stop = False
        daemon.shutdown()

    signal.signal(signal.SIGINT, signal_handler)

    try:
        daemon.requestLoop(loopCondition=check_stop)
    finally:
        ns.remove(name=Constant.NAMESPACE_GS+"."+node.getname()+str(oid))



if __name__=="__main__":
    main()
