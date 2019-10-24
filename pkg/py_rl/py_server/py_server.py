from concurrent import futures
import time
import logging
import random
import os

import grpc

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


_ONE_DAY_IN_SECONDS = 60 * 60 * 24

node_ids = []
task_ids = []

LOG_FILE = "../logs/alpha_log.log"

LOG_FORMAT = "[%(levelname)s]: %(asctime)s, %(message)s"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format=LOG_FORMAT)


def get_node_string(prefix, request):
    node_str = "{}\tuuid: {}\n\
            {}\tfriendly_name: {}\n\
            {}\tresource_type: {}\n\
            {}\tstate: {}\n\
            {}\tlabels: {}\n\
            {}\ttaints: {}\n\
            {}\tResourceCapacity:\n\
            {}\t\tRamCap: {}\n\
            {}\t\tCpuCores: {}\n\
            {}\t\tEphemeralCap(ephemeral storage): {}\n\
            {}\tAvailableResources:\n\
            {}\t\tRamCap: {}\n\
            {}\t\tCpuCores: {}\n\
            {}\t\tEphemeralCap(ephemeral storage): {}\n\
            {}\tReservedResources:\n\
            {}\t\tRamCap: {}\n\
            {}\t\tCpuCores: {}\n\
            {}\t\tEphemeralCap(ephemeral storage): {}\n\
            {}\tmax_pod: {}\n\
            {}\tavoidPods: {}\n\
            {}\tparent_id: {}\n\
            \n".format(prefix, request.resource_desc.uuid,
                       prefix, request.resource_desc.friendly_name,
                       prefix, resource_desc_pb2.ResourceDescriptor.ResourceType.Name(request.resource_desc.type),
                       prefix, resource_desc_pb2.ResourceDescriptor.ResourceState.Name(request.resource_desc.state),
                       prefix, get_label_str(request.resource_desc.labels),
                       prefix, request.resource_desc.taints,
                       prefix,
                       prefix, request.resource_desc.resource_capacity.ram_cap,
                       prefix, request.resource_desc.resource_capacity.cpu_cores,
                       prefix, request.resource_desc.resource_capacity.ephemeral_cap,
                       prefix,
                       prefix, request.resource_desc.available_resources.ram_cap,
                       prefix, request.resource_desc.available_resources.cpu_cores,
                       prefix, request.resource_desc.available_resources.ephemeral_cap,
                       prefix,
                       prefix, request.resource_desc.reserved_resources.ram_cap,
                       prefix, request.resource_desc.reserved_resources.cpu_cores,
                       prefix, request.resource_desc.reserved_resources.ephemeral_cap,
                       prefix, request.resource_desc.max_pods,
                       prefix, request.resource_desc.avoids,
                       prefix, request.parent_id)

    if len(request.children) > 0:
        for i, child in enumerate(request.children):
            node_str += "{}+++++Child {}+++++\n".format(prefix, i)
            prefix += "\t"
            node_str += get_node_string(prefix, child)
    return node_str


def log_added_node(request):
    log_str = "=====NODE ADDED=====\n"
    log_str += get_node_string("", request)
    log_str += "\n"
    logging.info(log_str)


def log_updated_node(request):
    log_str = "=====NODE UPDATED=====\n"
    log_str += get_node_string("", request)
    log_str += "\n"
    logging.info(log_str)


def get_label_str(labels):
    label_str = ""
    for i, label in enumerate(labels):
        label_str += "[LABEL {}] {} = {}; ".format(i, label.key, label.value)
    return label_str


def get_toleration_str(tolerations):
    tolerations_str = ""
    for i, toleration in enumerate(tolerations):
        tolerations_str += "[TOLERATION {}] {}<{}>{}, {}; ".format(
            i, toleration.key, toleration.operator, toleration.value, toleration.effect
        )
    return tolerations_str


def get_node_affinity_str(node_affinity):
    node_affinity_str = "NODE AFFINITIES: "

    # Required DuringSchedulingIgnoredDuringExecution
    node_affinity_str += "[REQUIRED] DuringSchedulingIgnoredDuringExecution: "
    require_selector = node_affinity.requiredDuringSchedulingIgnoredDuringExecution
    for i, selector_term in enumerate(require_selector.nodeSelectorTerms):
        for j, selector_requirement in enumerate(selector_term.matchExpressions):
            node_affinity_str += "Requirement({}) in Term({}): {}<{}>{}; ".format(
                j, i, selector_requirement.key, selector_requirement.operator, selector_requirement.values
            )

    # Preferred DuringSchedulingIgnoredDuringExecution
    node_affinity_str += "[PREFERRED] DuringSchedulingIgnoredDuringExecution: "
    for i, preferred_scheduling_term in enumerate(node_affinity.preferredDuringSchedulingIgnoredDuringExecution):
        for j, selector_requirement in enumerate(preferred_scheduling_term.preference.matchExpressions):
            node_affinity_str += "Requirement({}) in Term({}, weight {}): {}<{}>{}; ".format(
                j, i, preferred_scheduling_term.weight, selector_requirement.key, selector_requirement.operator,
                selector_requirement.values
            )

    return node_affinity_str


def get_pod_affinity_term_str(pod_affinity_term):
    pod_affinity_term_str = ""
    # PodLabelSelector
    pod_affinity_term_str += "PodLabelSelector: "
    pod_affinity_term_str += "{Match Labels: ({})=value({}); ".format(
        pod_affinity_term.labelSelector.matchLabels.key,
        pod_affinity_term.labelSelector.matchLabels.value,
    )
    for j, requirement in enumerate(pod_affinity_term.labelSelector.matchExpressions):
        pod_affinity_term_str += "requirement({}): {}<{}>{}; ".format(
            j, requirement.key, requirement.operator, requirement.values)
    pod_affinity_term_str += "}"

    pod_affinity_term_str += "Namespaces: "
    for j, namespace in pod_affinity_term.namespaces:
        pod_affinity_term_str += namespace + "; "

    pod_affinity_term_str += "topologyKey: {}; ".format(pod_affinity_term.topologyKey)


def get_pod_affinity_str(pod_affinity):
    pod_affinity_str = "POD AFFINITIES: "

    # Required DuringSchedulingIgnoredDuringExecution
    for i, pod_affinity_term in enumerate(pod_affinity.requiredDuringSchedulingIgnoredDuringExecution):
        pod_affinity_str += "[REQUIRE term]-{}: ".format(i)
        pod_affinity_str += get_pod_affinity_term_str(pod_affinity_term)

    # Preferred DuringSchedulingIgnoredDuringExecution
    for i, weighted_pod_affinity_term in enumerate(pod_affinity.preferredDuringSchedulingIgnoredDuringExecution):
        pod_affinity_str += "[PREFERRED term]-{}-weight({})： ".format(i, weighted_pod_affinity_term.weight)
        pod_affinity_str += get_pod_affinity_term_str(weighted_pod_affinity_term.podAffinityTerm)


def get_task_desc_str(task_descriptor):
    task_str = "======Task Descriptor======\n" \
               "\tuid: {}\n" \
               "\tname: {}\n" \
               "\tnamespace: {}\n" \
               "\tstate: {}\n" \
               "\ttask_type: {}\n" \
               "\tjob_id: {}\n" \
               "\tResourceRequest: \n" \
               "\t\tCpuCores: {}\n" \
               "\t\tRamCap: {}\n" \
               "\t\tEphemeralCap: {}\n" \
               "\towner_ref_kind: {}\n" \
               "\towner_ref_uid: {}\n" \
               "\tlabels: {}\n" \
               "\ttolerations: {}\n" \
               "\tnode_affinity: {}\n" \
               "\tpod_affinity: {}\n" \
               "\tpod_anti_affinity: {}" \
        .format(task_descriptor.uid,
                task_descriptor.name,
                task_descriptor.namespace,
                task_desc_pb2.TaskDescriptor.TaskState.Name(task_descriptor.state),
                task_desc_pb2.TaskDescriptor.TaskType.Name(task_descriptor.task_type),
                task_descriptor.job_id,
                task_descriptor.resource_request.cpu_cores,
                task_descriptor.resource_request.ram_cap,
                task_descriptor.resource_request.ephemeral_cap,
                task_descriptor.owner_ref_kind,
                task_descriptor.owner_ref_uid,
                get_label_str(task_descriptor.labels),
                get_toleration_str(task_descriptor.toleration),
                get_node_affinity_str(task_descriptor.affinity.node_affinity),
                get_pod_affinity_str(task_descriptor.affinity.pod_affinity),
                get_pod_affinity_str(task_descriptor.affinity.pod_anti_affinity)
                )

    return task_str


def get_job_desc_str(job_descriptor):
    job_str = "======Job Descriptor======\n" \
              "\tuuid: {}\n" \
              "\tname: {}\n" \
              "\tstate: {}\n" \
              "\tis_gang_scheduling_job: {}\n" \
              "\tmin_number_of_tasks: {}\n" \
        .format(
        job_descriptor.uuid,
        job_descriptor.name,
        job_desc_pb2.JobDescriptor.JobState.Name(job_descriptor.state),
        job_descriptor.is_gang_scheduling_job,
        job_descriptor.min_number_of_tasks
    )

    return job_str


def log_submitted_task(request):
    log_str = "=====TASK ADDED=====\n"
    log_str += get_task_desc_str(request.task_descriptor)
    log_str += get_job_desc_str(request.job_descriptor)
    log_str += "\n"
    logging.info(log_str)


def log_task_stats(task_stat):
    log_str = "=====TASK STATS=====\n" \
              "\ttask_id: {}\n" \
              "\thostname: {}\n" \
              "\ttimestamp: {}\n" \
              "\t**[Note]: CPU stats in millicores.\n" \
              "\tcpu_limit: {}\n" \
              "\tcpu_request: {}\n" \
              "\tcpu_usage: {}\n" \
              "\t**[Note]: Memory stats in Kb.\n" \
              "\tmem_limit: {}\n" \
              "\tmem_request: {}\n" \
              "\tmem_usage: {}\n" \
              "\tmem_rss: {}\n" \
              "\tmem_cache: {}\n" \
              "\tmem_working_set: {}\n" \
              "\tmem_page_faults: {}\n" \
              "\tmem_page_faults_rate: {}\n" \
              "\tmajor_page_faults: {}\n" \
              "\tmajor_page_faults_rate: {}\n" \
              "\t**[Note]: Network stats in Kb.\n" \
              "\tnet_rx: {}\n" \
              "\tnet_rx_errors: {}\n" \
              "\tnet_rx_errors_rate: {}\n" \
              "\tnet_rx_rate: {}\n" \
              "\tnet_tx: {}\n" \
              "\tnet_tx_errors: {}\n" \
              "\tnet_tx_errors_rate: {}\n" \
              "\tnet_tx_rate: {}\n" \
              "\n" \
        .format(
        task_stat.task_id,
        task_stat.hostname,
        task_stat.timestamp,
        task_stat.cpu_limit,
        task_stat.cpu_request,
        task_stat.cpu_usage,
        task_stat.mem_limit,
        task_stat.mem_request,
        task_stat.mem_usage,
        task_stat.mem_rss,
        task_stat.mem_cache,
        task_stat.mem_working_set,
        task_stat.mem_page_faults,
        task_stat.mem_page_faults_rate,
        task_stat.major_page_faults,
        task_stat.major_page_faults_rate,
        task_stat.net_rx,
        task_stat.net_rx_errors,
        task_stat.net_rx_errors_rate,
        task_stat.net_rx_rate,
        task_stat.net_tx,
        task_stat.net_tx_errors,
        task_stat.net_tx_errors_rate,
        task_stat.net_tx_rate,
    )
    logging.info(log_str)


def log_node_stats(resource_stat):
    cpus_stats_str = ""

    for i, cpu_stat in enumerate(resource_stat.cpus_stats):
        cpus_stats_str += "Core-{}: Millicores: cpu_capacity={}, cpu_allocatable={}; " \
                         "Fraction: cpu_reservation={}, cpu_utilization={}; ".format(
            i,
            cpu_stat.cpu_capacity,
            cpu_stat.cpu_allocatable,
            cpu_stat.cpu_reservation,
            cpu_stat.cpu_utilization,
        )

    log_str = "=====NODE STATS=====\n" \
              "\t**[Note]: resource_id is used to uniquely identify a resource.\n" \
              "\tresource_id: {}\n" \
              "\ttimestamp: {}\n" \
              "\t**[Note]: cpus_stats stores the stats of each CPU. The first entry is the cpu usage of cpu0 and so on.\n" \
              "\tcpu_stats: {}\n" \
              "\t**[Note]: Below are the Memory status (in KB) of node.\n" \
              "\t**[Note]: mem_allocatable is the allocatable memory resource of node.\n" \
              "\tmem_allocatable: {}\n" \
              "\t**[Note]: mem_capacity is the capacity of memory of node.\n" \
              "\tmem_capacity: {}\n" \
              "\t**[Note]: Memory stats (fraction of total).\n" \
              "\t**[Note]: mem_reservation is the fraction of memory reserved.\n" \
              "\tmem_reservation: {}\n" \
              "\t**[Note]: mem_utilization is the fraction of memory used.\n" \
              "\tmem_utilization: {}\n" \
              "\t**[Note]: Disk stats in KB.\n" \
              "\tdisk_bw: {}\n" \
              "\t**[Note]: Network stats in KB.\n" \
              "\t**[Note]: net_rx_bw is received network packets in KB.\n" \
              "\tnet_rx_bw: {}\n" \
              "\t**[Note]: net_tx_bw is transmit network packets in KB.\n" \
              "\tnet_tx_bw: {}\n" \
        .format(
        resource_stat.resource_id,
        resource_stat.timestamp,
        cpus_stats_str,
        resource_stat.mem_allocatable,
        resource_stat.mem_capacity,
        resource_stat.mem_reservation,
        resource_stat.mem_utilization,
        resource_stat.disk_bw,
        resource_stat.net_rx_bw,
        resource_stat.net_tx_bw
    )
    logging.info(log_str)


class FirmamentSchedulerServicer(firmament_scheduler_pb2_grpc.FirmamentSchedulerServicer):
    def Schedule(self, request, context):
        """Schedule sends a schedule request to firmament server.
            *=*=*=====Alpha=====*=*=*
        """
        deltas = []
        log_str = "=====Schedule request received=====\n"
        while len(task_ids) > 0:
            task_id = task_ids.pop(0)
            node_id = node_ids[random.randint(0, len(node_ids) - 1)]
            log_str += "\tPlace task {} to node {}\n".format(task_id, node_id)
            delta = scheduling_delta_pb2.SchedulingDelta(
                task_id=task_id,
                resource_id=node_id,
                type='PLACE')
            deltas.append(delta)
        log_str += "\n"
        logging.info(log_str)
        return firmament_scheduler_pb2.SchedulingDeltas(deltas=deltas, unscheduled_tasks=[])

    def TaskCompleted(self, request, context):
        """TaskCompleted notifies firmament server the given task is completed.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def TaskFailed(self, request, context):
        """TaskFailed notifies firmament server the given task is failed.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def TaskRemoved(self, request, context):
        """TaskRemoved notifies firmament server the given task is removed.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def TaskSubmitted(self, request, context):
        """TaskSubmitted notifies firmament server the given task is submitted.
            *=*=*=====Alpha=====*=*=*
        """
        task_ids.append(request.task_descriptor.uid)
        log_submitted_task(request)
        return firmament_scheduler_pb2.TaskSubmittedResponse(type='TASK_SUBMITTED_OK')

    def TaskUpdated(self, request, context):
        """TaskUpdated notifies firmament server the given task is updated.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def NodeAdded(self, request, context):
        """NodeAdded notifies firmament server the given node is added.
            *=*=*=====Alpha=====*=*=*
        """
        node_ids.append(request.resource_desc.uuid)
        log_added_node(request)
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
        log_updated_node(request)
        return firmament_scheduler_pb2.NodeAddedResponse(type='NODE_UPDATED_OK')

    def AddTaskStats(self, request, context):
        """AddTaskStats sends task status to firmament server.
            *=*=*=====Alpha=====*=*=*
        """
        log_task_stats(request)
        return firmament_scheduler_pb2.TaskStatsResponse(type='TASK_SUBMITTED_OK')

    def AddNodeStats(self, request, context):
        """AddNodeStats sends node status to firmament server.
            *=*=*=====Alpha=====*=*=*
        """
        log_node_stats(request)
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
        log_str = "=====TaskInfo Added=====\n" \
                  "\ttask_name: {}\n" \
                  "\tresource_id: {}\n" \
                  "\tcpu_utilization: {}\n" \
                  "\tmem_utilization: {}\n" \
                  "\tephemeral_storage_utilization: {}\n" \
                  "\ttype: {}" \
            .format(
            request.task_name,
            request.resource_id,
            request.cpu_utilization,
            request.mem_utilization,
            request.ephemeral_storage_utilization,
            firmament_scheduler_pb2.TaskInfoType.Name(request.type)
        )
        log_str += "\n"
        logging.info(log_str)
        if request.type == firmament_scheduler_pb2.TaskInfoType.Value('TASKINFO_ADD'):
            rtn_type = 'TASKINFO_SUBMITTED_OK'
        else:
            rtn_type = 'TASKINFO_REMOVED_OK'
        return firmament_scheduler_pb2.TaskInfoResponse(type=rtn_type)


def serve():
    # gRPC 服务器
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    firmament_scheduler_pb2_grpc.add_FirmamentSchedulerServicer_to_server(FirmamentSchedulerServicer(), server)
    server.add_insecure_port('[::]:9090')
    server.start()  # start() 不会阻塞，如果运行时你的代码没有其它的事情可做，你可能需要循环等待。
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        print("QAQ YOU ARE SO RUDE!!!!!")
        server.stop(0)


if __name__ == '__main__':
    with open(LOG_FILE, 'r+') as f:
        f.truncate()
    logging.info("Server started")
    start_time = time.strftime("%Y-%m-%d-%H_%M_%S", time.localtime())

    serve()

    logging.info("Server closed")
    end_time = time.strftime("%Y-%m-%d-%H_%M_%S", time.localtime())
    tgt_log_file = "../logs/log_[{}]_TO_[{}].log".format(start_time, end_time)
    os.system("cp {} {}".format(LOG_FILE, tgt_log_file))
