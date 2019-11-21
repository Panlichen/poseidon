import logging
import py_rl.py_server.scheduler_interface as scheduler_interface


class PodDict:
    def __init__(self):
        self.uid2raw = {}

    def add(self, raw_pod):
        self.uid2raw.setdefault(raw_pod.uid, raw_pod)

    def pop(self, uid):
        self.uid2raw.pop(uid, None)

    def items(self):
        return list(self.uid2raw.items())

    def keys(self):
        return list(self.uid2raw.keys())

    def values(self):
        return list(self.uid2raw.values())

class NodeDict:
    def __init__(self):
        self.uuid2raw = {}

    def add(self, raw_node):
        self.uuid2raw.setdefault(raw_node.uuid, raw_node)

    def pop(self, uuid):
        self.uuid2raw.pop(uuid, None)

    def items(self):
        return list(self.uuid2raw.items())

    def keys(self):
        return list(self.uuid2raw.keys())

    def values(self):
        return list(self.uuid2raw.values())


class RawPodInfo:
    """
    [OK] MilliCPU-request
	[OK] Memory-request
	[OK] EphemeralStorage-request
	[CANCEL] MilliCPU/Memory/EphemeralStorage-limit
    [CANCEL] PodCnt-1
    [OK] Jobinfo
    [TODO] label-key, value(should spread a wide area, may use hash)
    [TODO] namespace(should spread a wide area, may use hash)
    """
    def __init__(self, request):
        """
        Created when TaskSubmitted
        :param request: message TaskDescription
        """
        self.request = request

        # TODO: may need map between pod vector and its uid/name
        self.uid = request.task_descriptor.uid
        self.name = request.task_descriptor.name

        self.raw_namespace = request.task_descriptor.namespace
        self.namespace = self.get_spread_ns()

        self.MilliCPU_request = request.task_descriptor.resource_request.cpu_cores
        self.mem_request = request.task_descriptor.resource_request.ram_cap
        self.ephemeral_request = request.task_descriptor.resource_request.ephemeral_cap
        # MAY NOT TODO: NO resource limit info in firmament grpc proto file

        self.raw_job_id = request.task_descriptor.job_id
        self.job_id = self.get_spread_job_id()

        self.raw_label_str = ""
        self.num_labels = 30
        self.labels = self.process_pod_label()

        # from AddTaskStats
        self.task_stat_hostname = ""
        self.task_stat_cpu_limit = ""
        self.task_stat_cpu_request = ""  # TODO: requests here may be redundant
        self.task_stat_cpu_usage = ""
        self.task_stat_mem_limit = ""
        self.task_stat_mem_request = ""  # TODO: requests here may be redundant
        self.task_stat_mem_usage = ""
        self.task_stat_mem_rss = ""
        self.task_stat_mem_cache = ""
        self.task_stat_mem_working_set = ""
        self.task_stat_mem_page_faults = ""
        self.task_stat_mem_page_faults_rate = ""
        self.task_stat_major_page_faults = ""
        self.task_stat_major_page_faults_rate = ""

        # k8s default schedule policies do not use these:
        self.task_stat_net_rx = ""
        self.task_stat_net_rx_errors = ""
        self.task_stat_net_rx_errors_rate = ""
        self.task_stat_net_rx_rate = ""
        self.task_stat_net_tx = ""
        self.task_stat_net_tx_errors = ""
        self.task_stat_net_tx_errors_rate = ""
        self.task_stat_net_tx_rate = ""

        # for vector
        self.vec = [0] * scheduler_interface.Scheduler.pod_vector_len
        self.vidx_label_start = 5
        self.vidx_stats_start = self.vidx_label_start + self.num_labels

        self.make_pod_vector()

    def adjust_task_stats(self, task_stat):
        self.task_stat_hostname = task_stat.hostname
        self.task_stat_cpu_limit = task_stat.cpu_limit
        self.task_stat_cpu_request = task_stat.cpu_request
        self.task_stat_cpu_usage = task_stat.cpu_usage
        self.task_stat_mem_limit = task_stat.mem_limit
        self.task_stat_mem_request = task_stat.mem_request
        self.task_stat_mem_usage = task_stat.mem_usage
        self.task_stat_mem_rss = task_stat.mem_rss
        self.task_stat_mem_cache = task_stat.mem_cache
        self.task_stat_mem_working_set = task_stat.mem_working_set
        self.task_stat_mem_page_faults = task_stat.mem_page_faults
        self.task_stat_mem_page_faults_rate = task_stat.mem_page_faults_rate
        self.task_stat_major_page_faults = task_stat.major_page_faults
        self.task_stat_major_page_faults_rate = task_stat.major_page_faults_rate
        self.task_stat_net_rx = task_stat.net_rx
        self.task_stat_net_rx_errors = task_stat.net_rx_errors
        self.task_stat_net_rx_errors_rate = task_stat.net_rx_errors_rate
        self.task_stat_net_rx_rate = task_stat.net_rx_rate
        self.task_stat_net_tx = task_stat.net_tx
        self.task_stat_net_tx_errors = task_stat.net_tx_errors
        self.task_stat_net_tx_errors_rate = task_stat.net_tx_errors_rate
        self.task_stat_net_tx_rate = task_stat.net_tx_rate

        self.adjust_pod_stat_vec()

    def get_spread_ns(self):
        # TODO: complete get_spread_ns
        return hash(self.raw_namespace)

    def get_spread_job_id(self):
        """
        TaskUID is a uint64, but job_uuid is a string
        :return:
        """
        # TODO: complete get_spread_job_id
        return hash(self.raw_job_id)

    def process_pod_label(self):
        """
        0-9: common labels[death-star-project, app-name]
        10-19: input names
        20-29: output names
        :return:
        """
        labels = [0] * self.num_labels
        # TODO: return "standard" spread key-value
        common_idx = 0
        in_idx = 10
        out_idx = 20
        for i, label in enumerate(self.request.task_descriptor.labels):
            if i >= self.num_labels:
                logging.warning("Too many labels for pod. Now only support {}".format(self.num_labels))
                break
            self.raw_label_str += "[LABEL {}] {} = {}; ".format(i, label.key, label.value)
            if label.key.find("INPUT") != -1:
                labels[in_idx] = hash(label.value)
                in_idx += 1
            elif label.key.find("OUTPUT") != -1:
                labels[out_idx] = hash(label.value)
                out_idx += 1
            else:
                labels[common_idx] = hash(label.value)
                common_idx += 1

        return labels

    # TODO: complete logic when TaskCompleted, TaskFailed, TaskRemoved(their parameters are all TaskUID)
    # MAY NOT TODO: TaskUpdated, may be useless

    def adjust_pod_stat_vec(self):
        idx = self.vidx_stats_start
        self.vec[idx] = self.task_stat_cpu_limit; idx += 1
        self.vec[idx] = self.task_stat_cpu_request; idx += 1  # TODO: requests here may be redundant
        self.vec[idx] = self.task_stat_cpu_usage; idx += 1
        self.vec[idx] = self.task_stat_mem_limit; idx += 1
        self.vec[idx] = self.task_stat_mem_request; idx += 1  # TODO: requests here may be redundant
        self.vec[idx] = self.task_stat_mem_usage; idx += 1
        self.vec[idx] = self.task_stat_mem_rss; idx += 1
        self.vec[idx] = self.task_stat_mem_cache; idx += 1
        self.vec[idx] = self.task_stat_mem_working_set; idx += 1
        self.vec[idx] = self.task_stat_mem_page_faults; idx += 1
        self.vec[idx] = self.task_stat_mem_page_faults_rate; idx += 1
        self.vec[idx] = self.task_stat_major_page_faults; idx += 1
        self.vec[idx] = self.task_stat_major_page_faults_rate; idx += 1

        # k8s default schedule policies do not use these:
        self.vec[idx] = self.task_stat_net_rx; idx += 1
        self.vec[idx] = self.task_stat_net_rx_errors; idx += 1
        self.vec[idx] = self.task_stat_net_rx_errors_rate; idx += 1
        self.vec[idx] = self.task_stat_net_rx_rate; idx += 1
        self.vec[idx] = self.task_stat_net_tx; idx += 1
        self.vec[idx] = self.task_stat_net_tx_errors; idx += 1
        self.vec[idx] = self.task_stat_net_tx_errors_rate; idx += 1
        self.vec[idx] = self.task_stat_net_tx_rate; idx += 1

    def make_pod_vector(self):
        idx = 0
        self.vec[idx] = self.namespace; idx += 1
        self.vec[idx] = self.MilliCPU_request; idx += 1
        self.vec[idx] = self.mem_request; idx += 1
        self.vec[idx] = self.ephemeral_request; idx += 1
        self.vec[idx] = self.job_id; idx += 1

        for i in range(self.num_labels):
            self.vec[self.vidx_label_start + i] = self.labels[i]


class RawNodeInfo:
    """
    [OK] MilliCPU-allocatable
	[OK] Memory-allocatable
	[OK] EphemeralStorage-allocatable
    [OK] AllowedPodNumber
    [TODO] node label-key, value(should spread a wide area, may use hash)

    REAL TIME STATUS:
        [MAY OK] MilliCPU-total request -- allocatable * utilization?
        [MAY OK] Memory-total request;
        [MAY NOT TODO] EphemeralStorage-total request;
        [MAY OK] CPU fraction --  get utilization from node stat
        [MAY OK] memory fraction
        [MAY NOT TODO] ephemeral storage fraction
        [TODO] podnumber fraction?
        [TODO] all pods on a node -- use task stats' host name to get
    """
    def __init__(self, request):
        self.request = request
        self.uuid = request.resource_desc.uuid
        self.name = request.resource_desc.friendly_name

        self.allocatable_MilliCPU = request.resource_desc.available_resources.cpu_cores
        self.allocatable_memory = request.resource_desc.available_resources.ram_cap
        self.allocatable_ephemeral = request.resource_desc.available_resources.ephemeral_cap
        self.allowed_pod_num = request.resource_desc.max_pods

        self.raw_label_str = ""
        self.num_labels = 10
        self.labels = self.process_node_label()

        # from AddNodeStats
        self.resource_stat_resource_id = ""
        self.resource_stat_cpu_allocatable = ""  # TODO: allocatable here may be redundant
        self.resource_stat_cpu_utilization = ""
        self.cpu_total_request = ""
        self.resource_stat_mem_allocatable = ""  # TODO: allocatable here may be redundant
        self.resource_stat_mem_utilization = ""
        self.mem_total_request = ""

        # k8s default schedule policies do not use these:
        self.resource_stat_disk_bw = ""
        self.resource_stat_net_rx_bw = ""
        self.resource_stat_net_tx_bw = ""

        # maintain by hand
        self.current_pod_num = 0
        self.pods = PodDict()

        # for vector
        self.vec = [0] * scheduler_interface.Scheduler.node_vector_len
        self.vidx_label_start = 4
        self.vidx_stat_start = self.vidx_label_start + self.num_labels
        self.make_node_vector()

    def adjust_node_stat_vector(self):
        idx = self.vidx_stat_start
        self.vec[idx] = self.current_pod_num; idx += 1
        self.vec[idx] = self.resource_stat_cpu_allocatable; idx += 1  # TODO: allocatable here may be redundant
        self.vec[idx] = self.resource_stat_cpu_utilization; idx += 1
        self.vec[idx] = self.cpu_total_request; idx += 1
        self.vec[idx] = self.resource_stat_mem_allocatable; idx += 1  # TODO: allocatable here may be redundant
        self.vec[idx] = self.resource_stat_mem_utilization; idx += 1
        self.vec[idx] = self.mem_total_request; idx += 1

        # k8s default schedule policies do not use these:
        self.vec[idx] = self.resource_stat_disk_bw; idx += 1
        self.vec[idx] = self.resource_stat_net_rx_bw; idx += 1
        self.vec[idx] = self.resource_stat_net_tx_bw; idx += 1


    def make_node_vector(self):
        idx = 0
        self.vec[idx] = self.allocatable_MilliCPU; idx += 1
        self.vec[idx] = self.allocatable_memory; idx += 1
        self.vec[idx] = self.allocatable_ephemeral; idx += 1
        self.vec[idx] = self.allowed_pod_num; idx += 1

        for i in range(self.num_labels):
            self.vec[self.vidx_label_start + i] = self.labels[i]

    def process_node_label(self):
        """
        There are 6 default labels
        [LABEL 0] kubernetes.io/hostname = 10.0.0.37; [LABEL 1] kubernetes.io/os = linux; [LABEL 2] kubernetes.io/role = node;
        [LABEL 3] beta.kubernetes.io/arch = amd64; [LABEL 4] beta.kubernetes.io/os = linux; [LABEL 5] kubernetes.io/arch = amd64;
        :return:
        """
        # TODO: return "standard" spread key-value
        labels = [0] * self.num_labels
        for i, label in enumerate(self.request.resource_desc.labels):
            if i > self.num_labels:
                logging.warning("Too many labels for node,Now only support 10")
                labels[i] = hash(label.value)
            self.raw_label_str += "[LABEL {}] {} = {}; ".format(i, label.key, label.value)
        return labels

    def adjust_node_stats(self, resource_stat):
        self.resource_stat_resource_id = resource_stat.resource_id
        self.resource_stat_cpu_allocatable = resource_stat.cpus_stats[0].cpu_allocatable
        self.resource_stat_cpu_utilization = resource_stat.cpus_stats[0].cpu_utilization
        self.cpu_total_request = self.resource_stat_cpu_allocatable * self.resource_stat_cpu_utilization
        self.resource_stat_mem_allocatable = resource_stat.mem_allocatable
        self.resource_stat_mem_utilization = resource_stat.mem_utilization
        self.mem_total_request = self.resource_stat_mem_allocatable * self.resource_stat_mem_utilization
        self.resource_stat_disk_bw = resource_stat.disk_bw
        self.resource_stat_net_rx_bw = resource_stat.net_rx_bw
        self.resource_stat_net_tx_bw = resource_stat.net_tx_bw

        self.adjust_node_stat_vector()

    def increase_pod_num(self):
        self.current_pod_num += 1

    def decrease_pod_num(self):
        self.current_pod_num -= 1

    def put_pod(self, raw_pod):
        self.pods.add(raw_pod)
        self.increase_pod_num()
        # TODO: determine whether handle "TaskInfo", i.e. existing pods, specially;
        # TODO: if not, then self.current_pod_num is useless

    def pop_pod(self, pod_uid):
        self.pods.pop(pod_uid)
        self.decrease_pod_num()
