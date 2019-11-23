import logging

import numpy as np
from py_rl.py_server.scheduler_interface import Scheduler
import py_rl.py_firmament_grpc.scheduling_delta_pb2 as scheduling_delta_pb2

class SimpleScheduler(Scheduler):
    def __init__(self):
        self.pod_matrix = np.random.rand(Scheduler.out_tensor_len, Scheduler.pod_vector_len)
        self.inner_pod_matrix = np.random.rand(Scheduler.out_tensor_len, Scheduler.pod_vector_len)
        self.node_matrix = np.random.rand(Scheduler.out_tensor_len, Scheduler.node_vector_len)

    def calc_pod_tensor(self, pod_vector):
        pod_tensor = self.pod_matrix.dot(np.array(pod_vector))
        return pod_tensor

    def calc_node_tensor(self, raw_node):
        """
        :param raw_node: raw_info.RawNodeInfo
        :return:
        """
        node_tensor = self.node_matrix.dot(np.array(raw_node.vec))
        pod_tensors = []
        for raw_pod in raw_node.pods.values():
            pod_tensors.append(self.inner_pod_matrix.dot(np.array(raw_pod.vec)))
        pod_tensors = np.array(pod_tensors)
        return node_tensor + pod_tensors.sum(axis=0)

    def schedule(self, nodes, pods_to_schedule, pods_running):
        # TODO: if schedule many pods to a node at once, may over use resources. rare but need to think about it
        deltas = []
        node_uuid2tensor = {}
        log_str = "=====In Simple Scheduler=====\n"

        if len(nodes.keys()) == 0 or len(pods_to_schedule.keys()) == 0:
            log_str += "\t Nothing to do\n"
            logging.info(log_str)

            return deltas

        for uuid, raw_node in nodes.items():
            node_uuid2tensor.setdefault(uuid, self.calc_node_tensor(raw_node))

        for uid, raw_pod in pods_to_schedule.items():
            max_dot_prod = float("-inf")
            pod_tensor = self.calc_pod_tensor(raw_pod.vec)
            for uuid, node_tensor in node_uuid2tensor.items():
                dot_prod = pod_tensor.dot(node_tensor)  # TODO: note huge or minus hashed label
                if dot_prod > max_dot_prod:
                    max_dot_prod = dot_prod
                    tgt_uuid = uuid
                    tgt_node_tensor = node_tensor
            log_str += "\t pod_tensor: {}\n" \
                       "\t tgt_node_tensor: {}\n" \
                       "".format(pod_tensor.tolist(), tgt_node_tensor.tolist())
            log_str += "\t Place task {} to node {}\n".format(uid, tgt_uuid)
            delta = scheduling_delta_pb2.SchedulingDelta(
                task_id=uid,
                resource_id=tgt_uuid,
                type='PLACE')
            deltas.append(delta)

            pods_running.add(raw_pod)
            pods_to_schedule.pop(raw_pod.uid)

        log_str += "\n"
        logging.info(log_str)

        return deltas







