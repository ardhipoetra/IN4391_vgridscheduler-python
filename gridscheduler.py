import Pyro4
import threading
import serpent
from node import Node
from resourcemanager import ResourceManager
from constant import Constant
from constant import Pool
import utils
import time
import signal
import sys
from operator import attrgetter
import random
import socket
import subprocess

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
            # self._write("[JOB_RUNNING] %s" %self.query_rmjob())
            self._write("[CLUSTERLOAD] %s" %self.query_rm())

   ## Define the data structure which maintains the state of each GS
    def __init__(self, oid, name="GS"):
        Node.__init__(self, oid, name)
        utils.write(Constant.NODE_GRIDSCHEDULER, self.oid, "created with id %d" %(oid))

        self.jobs_assigned_RM = utils.initarraylist_none(Constant.TOTAL_RM)
        self.RM_loads = [0.0 for i in range(Constant.TOTAL_RM)] #rm connected in this


        # thread = threading.Thread(target=self._periodicresult)
        # thread.setDaemon(True)
        # thread.start()

    # report received after finishing the task from RM
    @Pyro4.oneway
    def receive_report(self, rmid, d_job):
        job = serpent.loads(d_job)
        self._write('received report %s from RM-%d' %(job, rmid))

        # remove from stat monitor
        # print str(self.oid)+" dec  "+str(job["load"]/float(len(self.RM_loads)))
        self.RM_loads[rmid] -= (job["load"]/float(len(self.RM_loads)))
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
        self._write('job %d FINISHED at %f (%f)' %(job["jid"], timedone, tdiff))

        # if there's job in the queue
        if len(self.job_queue) != 0:
            self._write("queue not empty, try assign job to RM")
            rmidsub = self._chooseRM()
            if rmidsub == -1:
                self._write('no rm available!')
                return False
            else:
                jobsub = self._choose_job()

                if jobsub is None:
                    self._write("no job available")
                else:
                    jobsub["RM_assigned"] = rmidsub
                    self._write("job %d submitted to %d" %(jobsub["jid"], rmidsub))
                    self._assignjob(rmidsub, serpent.dumps(jobsub))

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

        with Pyro4.Proxy(utils.find(Constant.NODE_RESOURCEMANAGER, rmid)) as rmobj:
            self._write('send job %d to %s' % (job["jid"], rmobj.tostr()))

            # print str(self.oid)+" add : "+str(job["load"]/float(len(self.RM_loads)))
            self.RM_loads[rmid] += (job["load"]/float(len(self.RM_loads)))

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
        rm_tmp = [99.99] * Constant.TOTAL_RM

        for rmid in range(0, Constant.TOTAL_RM):
            struri = utils.find(Constant.NODE_RESOURCEMANAGER, rmid)
            if struri is None:
                continue
            try:
                with Pyro4.Proxy(struri) as rmobj:
                    rm_tmp[rmobj.getoid()] = rmobj.get_workloadRM()
            except Pyro4.errors.NamingError as e:
                continue


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

        for gsid in gs_listid:
            with Pyro4.Proxy(utils.find(Constant.NODE_GRIDSCHEDULER, gsid)) as gsobj :
                gsobj.get_structure(self.oid, msg_gs)

        return True

    # monitor GS to handle fault in GS
    def _monitorneighborGS(self):
        activeid = [self.oid]
        inactiveid = []
        for gid in range(0, Constant.TOTAL_GS):
            struri = utils.find(Constant.NODE_GRIDSCHEDULER, gid)
            if struri is None:
                continue
            try:
                with Pyro4.Proxy(struri) as gsobj :
                    if gsobj.getoid() != self.oid:
                        activeid.append(gsobj.getoid())
            except Pyro4.errors.NamingError as e:
                continue

        if len(activeid) != Constant.TOTAL_GS:
            inactiveid = list(set([x for x in range(0, Constant.TOTAL_GS)]) - set(activeid))

        self._write("active GS : %s | inactive GS : %s" %(str(activeid), str(inactiveid)))

        return (activeid, inactiveid)

    # monitor RM to handle fault in RM
    def _monitorRM(self):
        # only look at related RM
        for rmid, jobs_in_rm in enumerate(self.jobs_assigned_RM):
            if jobs_in_rm == [None]:
                continue

            try:
                self._write("monitoring check RM %d" %(rmid))
                struri = utils.find(Constant.NODE_RESOURCEMANAGER, rmid)
                if struri is None:
                    raise Exception('None in Pool')
                Pyro4.resolve(struri)

                with Pyro4.Proxy(struri) as rmobj:
                    pass
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


    #UNUSABLE FOR NOW
    def query_rmload(self):
        totalworkload = 0.0
        buff = ""
        for rmid in range(0, Constant.TOTAL_RM):
            struri = utils.find(Constant.NODE_RESOURCEMANAGER, rmid)
            if struri is None:
                continue
            with Pyro4.Proxy(struri) as rmobj:
                totalworkload += rmobj.get_workloadRM()
                buff += "[%d] %f," %(rmid, rmobj.get_workloadRM())

        return buff

    #UNUSABLE FOR NOW
    def query_rmjob(self):
        totaljobrunning = 0.0
        buff = ""
        for rmid in range(0, Constant.TOTAL_RM):
            struri = utils.find(Constant.NODE_RESOURCEMANAGER, rmid)
            if struri is None:
                continue
            with Pyro4.Proxy(struri) as rmobj:
                totaljobrunning += rmobj.get_totaljobs_run()
                buff += "[%d] %f," %(rmid, rmobj.get_totaljobs_run())

        return buff

    #UNUSABLE FOR NOW
    def query_rm(self):
        totaljobrunning = 0.0
        totalworkload = 0.0
        buff = ""
        for rmid in range(0, Constant.TOTAL_RM):
            struri = utils.find(Constant.NODE_RESOURCEMANAGER, rmid)
            if struri is None:
                continue
            with Pyro4.Proxy(struri) as rmobj:
                totalworkload += rmobj.get_workloadRM()
                totaljobrunning += rmobj.get_totaljobs_run()
                buff += "%f;%d, " %(rmobj.get_workloadRM(), rmobj.get_totaljobs_run())

        return buff

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

    #check for ns :
    nsready = 0
    hostname = socket.gethostname()
    gs_ip = Pyro4.socketutil.getIpAddress(hostname, workaround127=True)

    # spawn ns, probably can't find it
    try :
        Pyro4.locateNS(host=gs_ip)
        nsready = -1
    except Pyro4.errors.NamingError:
        subprocess.Popen(["python","-m","Pyro4.naming","--host="+gs_ip])
        nsready = 1


    # ns should be available
    while(nsready > 0):
        try:
            Pyro4.locateNS(host=gs_ip)
            nsready = -1
        except Pyro4.errors.NamingError:
            time.sleep(0.5)
            pass

    #ns should be ready by now
    if len(sys.argv) != 2:
        print "you must provide GS id"
        sys.exit()
    else:
        oid = int(sys.argv[1])

    node = GridScheduler(oid, "[GS-"+str(oid)+"]")
    daemon = Pyro4.Daemon(gs_ip)
    uri = daemon.register(node)
    node.seturi(uri)

    def signal_handler(signal, frame):
        utils.write(Constant.NODE_GRIDSCHEDULER, oid, "GS will down!")
        stop = False
        daemon.shutdown()

    with Pyro4.locateNS(host=gs_ip) as ns:
        ns.register(Constant.NAMESPACE_GS+"."+node.getname()+str(oid), uri)

    signal.signal(signal.SIGINT, signal_handler)

    check_env()

    print "[%f]-%d GS everything ready!" %(time.time(), oid)

    daemon.requestLoop(loopCondition=check_stop)

    with Pyro4.locateNS(host=gs_ip) as ns:
        ns.remove(Constant.NAMESPACE_GS+"."+node.getname()+str(oid))

def check_env():
    # check if all GS are ready
    lgs_tmp = [None] * Constant.TOTAL_GS
    ready = False
    while(not ready):
        for i in range(0,Constant.TOTAL_GS):
            for ip in Pool.POTENTIAL_LINK:
                try:
                    Pyro4.resolve("PYRONAME:%s.[GS-%d]%d@%s" %(Constant.NAMESPACE_GS,i,i,ip))
                    lgs_tmp[i] = ip
                except Pyro4.errors.NamingError:
                    pass


            if i == Constant.TOTAL_GS - 1:
                time.sleep(1)
                ready = (len([el for el in lgs_tmp if el is None]) == 0)

    for gid, gip in enumerate(lgs_tmp):
        if "gs-"+str(gid) not in Pool.lookuptable:
            Pool.lookuptable["gs-"+str(gid)] = gip

    # check if all RM are ready
    lrm_tmp = [None] * Constant.TOTAL_RM
    ready = False
    while(not ready):
        for i in range(0,Constant.TOTAL_RM):
            for ip in Pool.POTENTIAL_LINK:
                try:
                    Pyro4.resolve("PYRONAME:%s.[RM-%d]%d@%s" %(Constant.NAMESPACE_RM,i,i,ip))
                    lrm_tmp[i] = ip
                except Pyro4.errors.NamingError:
                    pass


            if i == Constant.TOTAL_RM - 1:
                time.sleep(1)
                ready = (len([el for el in lrm_tmp if el is None]) == 0)

    for rid, rip in enumerate(lrm_tmp):
        if ("rm-"+str(rid)) not in Pool.lookuptable:
            Pool.lookuptable["rm-"+str(rid)] = rip


if __name__=="__main__":
    main()
