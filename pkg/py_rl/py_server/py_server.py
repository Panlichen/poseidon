# import py_rl.py_firmament_grpc.affinity_pb2_grpc as affinity_pb2_grpc
# import py_rl.py_firmament_grpc.affinity_pb2 as affinity_pb2
# import py_rl.py_firmament_grpc.avoid_pods_annotation_pb2_grpc as avoid_pods_annotation_pb2_grpc
# import py_rl.py_firmament_grpc.avoid_pods_annotation_pb2 as avoid_pods_annotation_pb2
# import py_rl.py_firmament_grpc.coco_interference_scores_pb2_grpc as coco_interference_scores_pb2_grpc
# import py_rl.py_firmament_grpc.coco_interference_scores_pb2 as coco_interference_scores_pb2
import py_rl.py_firmament_grpc.firmament_scheduler_pb2_grpc as firmament_scheduler_pb2_grpc
import py_rl.py_firmament_grpc.firmament_scheduler_pb2 as firmament_scheduler_pb2
# import py_rl.py_firmament_grpc.job_desc_pb2_grpc as job_desc_pb2_grpc
import py_rl.py_firmament_grpc.job_desc_pb2 as job_desc_pb2
# import py_rl.py_firmament_grpc.label_pb2_grpc as label_pb2_grpc
# import py_rl.py_firmament_grpc.label_pb2 as label_pb2
# import py_rl.py_firmament_grpc.label_selector_pb2_grpc as label_selector_pb2_grpc
# import py_rl.py_firmament_grpc.label_selector_pb2 as label_selector_pb2
# import py_rl.py_firmament_grpc.node_affinity_pb2_grpc as node_affinity_pb2_grpc
# import py_rl.py_firmament_grpc.node_affinity_pb2 as node_affinity_pb2
# import py_rl.py_firmament_grpc.pod_affinity_pb2_grpc as pod_affinity_pb2_grpc
# import py_rl.py_firmament_grpc.pod_affinity_pb2 as pod_affinity_pb2
# import py_rl.py_firmament_grpc.pod_anti_affinity_pb2_grpc as pod_anti_affinity_pb2_grpc
# import py_rl.py_firmament_grpc.pod_anti_affinity_pb2 as pod_anti_affinity_pb2
# import py_rl.py_firmament_grpc.reference_desc_pb2_grpc as reference_desc_pb2_grpc
# import py_rl.py_firmament_grpc.reference_desc_pb2 as reference_desc_pb2
# import py_rl.py_firmament_grpc.resource_desc_pb2_grpc as resource_desc_pb2_grpc
import py_rl.py_firmament_grpc.resource_desc_pb2 as resource_desc_pb2
# import py_rl.py_firmament_grpc.resource_stats_pb2_grpc as resource_stats_pb2_grpc
# import py_rl.py_firmament_grpc.resource_stats_pb2 as resource_stats_pb2
# import py_rl.py_firmament_grpc.resource_topology_node_desc_pb2_grpc as resource_topology_node_desc_pb2_grpc
# import py_rl.py_firmament_grpc.resource_topology_node_desc_pb2 as resource_topology_node_desc_pb2
# import py_rl.py_firmament_grpc.resource_vector_pb2_grpc as resource_vector_pb2_grpc
# import py_rl.py_firmament_grpc.resource_vector_pb2 as resource_vector_pb2
# import py_rl.py_firmament_grpc.scheduling_delta_pb2_grpc as scheduling_delta_pb2_grpc
import py_rl.py_firmament_grpc.scheduling_delta_pb2 as scheduling_delta_pb2
# import py_rl.py_firmament_grpc.taints_pb2_grpc as taints_pb2_grpc
# import py_rl.py_firmament_grpc.taints_pb2 as taints_pb2
# import py_rl.py_firmament_grpc.task_desc_pb2_grpc as task_desc_pb2_grpc
import py_rl.py_firmament_grpc.task_desc_pb2 as task_desc_pb2

# import py_rl.py_firmament_grpc.task_final_report_pb2_grpc as task_final_report_pb2_grpc
# import py_rl.py_firmament_grpc.task_final_report_pb2 as task_final_report_pb2
# import py_rl.py_firmament_grpc.task_stats_pb2_grpc as task_stats_pb2_grpc
# import py_rl.py_firmament_grpc.task_stats_pb2 as task_stats_pb2
# import py_rl.py_firmament_grpc.tolerations_pb2_grpc as tolerations_pb2_grpc
# import py_rl.py_firmament_grpc.tolerations_pb2 as tolerations_pb2
# import py_rl.py_firmament_grpc.whare_map_stats_pb2_grpc as whare_map_stats_pb2_grpc
# import py_rl.py_firmament_grpc.whare_map_stats_pb2 as whare_map_stats_pb2

from py_rl.py_server.logger import Logger
import py_rl.py_server.raw_info as raw_info
import py_rl.py_server.simple_scheduler as simple_scheduler

from concurrent import futures
import time
import logging
import os
import grpc
from tensorboardX import SummaryWriter


class FirmamentSchedulerServicer(firmament_scheduler_pb2_grpc.FirmamentSchedulerServicer):

    def __init__(self):
        self.scheduler = simple_scheduler.SimpleScheduler()

    def Schedule(self, request, context):
        """Schedule sends a schedule request to firmament server.
            *=*=*=====Alpha=====*=*=*
        """
        # context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        # context.set_details('Method not implemented!')
        # raise NotImplementedError('Method not implemented!')

        deltas = self.scheduler.schedule(nodes, pods_to_schedule, pods_running)

        # TODO: try to print things out of deltas——see how to use those data structures

        return firmament_scheduler_pb2.SchedulingDeltas(deltas=deltas, unscheduled_tasks=[])

        # deltas = []
        # log_str = "=====Schedule request received=====\n"
        # while len(task_ids) > 0:
        #     task_id = task_ids.pop(0)
        #     node_id = node_ids[random.randint(0, len(node_ids) - 1)]
        #     log_str += "\tPlace task {} to node {}\n".format(task_id, node_id)
        #     delta = scheduling_delta_pb2.SchedulingDelta(
        #         task_id=task_id,
        #         resource_id=node_id,
        #         type='PLACE')
        #     deltas.append(delta)
        # log_str += "\n"
        # logging.debug(log_str)
        # return firmament_scheduler_pb2.SchedulingDeltas(deltas=deltas, unscheduled_tasks=[])

    def TaskCompleted(self, request, context):
        """TaskCompleted notifies firmament server the given task is completed.
        """
        pods_running.pop(request.task_uid)
        logger.brief_log_task_completed(request.task_uid)
        return firmament_scheduler_pb2.TaskCompletedResponse(type='TASK_COMPLETED_OK')

    def TaskFailed(self, request, context):
        """TaskFailed notifies firmament server the given task is failed.
        """
        pods_running.pop(request.task_uid)
        logger.brief_log_task_failed(request.task_uid)
        return firmament_scheduler_pb2.TaskFailedResponse(type='TASK_FAILED_OK')

    def TaskRemoved(self, request, context):
        """TaskRemoved notifies firmament server the given task is removed.
        """
        pods_running.pop(request.task_uid)
        logger.brief_log_task_removed(request.task_uid)
        return firmament_scheduler_pb2.TaskRemovedResponse(type='TASK_REMOVED_OK')

    def TaskSubmitted(self, request, context):
        """TaskSubmitted notifies firmament server the given task is submitted.
        """
        new_raw_pod = raw_info.RawPodInfo(request)
        pods_to_schedule.add(new_raw_pod)
        logger.log_submitted_task(request)
        logger.brief_log_task_submitted(new_raw_pod)
        return firmament_scheduler_pb2.TaskSubmittedResponse(type='TASK_SUBMITTED_OK')

    def TaskUpdated(self, request, context):
        """TaskUpdated notifies firmament server the given task is updated.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def NodeAdded(self, request, context):
        """NodeAdded notifies firmament server the given node is added.
        """
        # skip closed server
        for _, taint in enumerate(request.resource_desc.taints):
            if taint.key == "closed":
                return firmament_scheduler_pb2.NodeAddedResponse(type='NODE_ADDED_OK')

        new_raw_node = raw_info.RawNodeInfo(request)
        nodes.add(new_raw_node)
        logger.log_added_node(request)
        logger.brief_log_node_added(new_raw_node)
        return firmament_scheduler_pb2.NodeAddedResponse(type='NODE_ADDED_OK')

    def NodeFailed(self, request, context):
        """NodeFailed notifies firmament server the given node is failed.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def NodeRemoved(self, request, context):
        """NodeRemoved notifies firmament server the given node is removed.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def NodeUpdated(self, request, context):
        """NodeUpdated notifies firmament server the given node is updated.
            *=*=*=====Alpha=====*=*=*
        """
        logger.log_updated_node(request)
        return firmament_scheduler_pb2.NodeAddedResponse(type='NODE_UPDATED_OK')

    def AddTaskStats(self, request, context):
        """AddTaskStats sends task status to firmament server.
        """
        global task_stat_epoch
        logger.log_task_stats(request)
        if request.task_id in pods_running.uid2raw.keys():
            raw_pod = pods_running.uid2raw[request.task_id]
            raw_pod.adjust_task_stats(request)
            logger.brief_log_task_stats(raw_pod, writer, start_time, task_stat_epoch)
            task_stat_epoch += 1
        return firmament_scheduler_pb2.TaskStatsResponse(type='TASK_SUBMITTED_OK')

    def AddNodeStats(self, request, context):
        """AddNodeStats sends node status to firmament server.
        """
        logger.log_node_stats(request)
        if request.resource_id in nodes.uuid2raw.keys():
            raw_node = nodes.uuid2raw[request.resource_id]
            raw_node.adjust_node_stats(request)
            logger.brief_log_node_stats(raw_node)
        return firmament_scheduler_pb2.ResourceStatsResponse(type='NODE_ADDED_OK')

    def Check(self, request, context):
        """Healthcheck
            *=*=*=====Alpha=====*=*=*
        """
        return firmament_scheduler_pb2.HealthCheckResponse(status='SERVING')

    def AddTaskInfo(self, request, context):
        """AddTaskInfo sends task status to firmament server.
            *=*=*=====Alpha=====*=*=*
        """
        # MAY NOT TODO: no task_uid info, just podname/ns;
        # MAY NOT TODO: may add to pods_running, but not very useful
        logger.log_add_task_info(request)
        if request.type == firmament_scheduler_pb2.TaskInfoType.Value('TASKINFO_ADD'):
            rtn_type = 'TASKINFO_SUBMITTED_OK'
        else:
            rtn_type = 'TASKINFO_REMOVED_OK'
        return firmament_scheduler_pb2.TaskInfoResponse(type=rtn_type)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    firmament_scheduler_pb2_grpc.add_FirmamentSchedulerServicer_to_server(FirmamentSchedulerServicer(), server)
    server.add_insecure_port('[::]:9090')
    server.start()
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        print("QAQ YOU ARE SO RUDE!!!!!")
        server.stop(0)


if __name__ == '__main__':

    start_time = time.strftime("%Y-%m-%d-%H_%M_%S", time.localtime())
    task_stat_epoch = 0

    # use tensorboardX to record stats
    writer = SummaryWriter("../../logs/board")

    logger = Logger()
    _ONE_DAY_IN_SECONDS = 60 * 60 * 24
    LOG_FILE = "../../logs/alpha_log.log"

    LOG_FORMAT = "[%(levelname)s]: %(asctime)s, %(message)s"
    logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format=LOG_FORMAT)

    with open(LOG_FILE, 'r+') as f:
        f.truncate()
    logging.info("Server started")

    # data structures
    nodes = raw_info.NodeDict()
    pods_to_schedule = raw_info.PodDict()
    pods_running = raw_info.PodDict()

    serve()

    logging.info("Server closed")
    end_time = time.strftime("%Y-%m-%d-%H_%M_%S", time.localtime())
    tgt_log_file = "../../logs/log_[{}]_TO_[{}].log".format(start_time, end_time)
    os.system("cp {} {}".format(LOG_FILE, tgt_log_file))
