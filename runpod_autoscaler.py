import json
from argparse import ArgumentParser
from collections import defaultdict
from itertools import chain
from pathlib import Path
from typing import Tuple

import yaml

import runpod
import attr
from attr.validators import instance_of
from clearml import Task
from clearml.automation.auto_scaler import AutoScaler, ScalerConfig
from clearml.automation.cloud_driver import CloudDriver, parse_tags
from clearml.config import running_remotely


DEFAULT_DOCKER_IMAGE = "ubuntu:22.04"

default_config = {
    "hyper_params": {
        "git_user": "",
        "git_pass": "",
        "runpod_api_key": "",
        "default_docker_image": DEFAULT_DOCKER_IMAGE,
        "max_idle_time_min": 5,
        "polling_interval_time_min": 2,
        "max_spin_up_time_min": 10,
        "workers_prefix": "runpod_worker",
        "cloud_provider": "RunPod",
    },
    "configurations": {
        "resource_configurations": {
            "A4000": {
                "instance_type": "A4000",
                "gpus": 1,
                "cpus": 1,
                "memory": 8,
                "gpu_id": "NVIDIA RTX A4000",
                "disk_container": 30,
                "disk_volume": 30,
                "cloud": "SECURE",
            },
            "3070": {
                "instance_type": "3070",
                "gpus": 1,
                "cpus": 1,
                "memory": 8,
                "gpu_id": "NVIDIA RTX 3070",
                "disk_container": 30,
                "disk_volume": 30,
                "cloud": "COMMUNITY",
            },
            "H100": {
                "instance_type": "H100",
                "gpus": 1,
                "cpus": 1,
                "memory": 8,
                "gpu_id": "NVIDIA H100 PCIe",
                "disk_container": 30,
                "disk_volume": 30,
                "cloud": "ALL",
                "template_id": "template_id",  # RunPod template ID
            }
        },
        "queues": {"main": [("A4000", 3), ("3070", 0)]},
        "extra_trains_conf": "",
        "extra_clearml_conf": "",
        "extra_vm_bash_script": "",
        "extra_env_vars": {
            "MINIO_URL": "",
            "MINIO_HOST": "",
            "MINIO_ACCESS_KEY": "",
            "MINIO_SECRET_KEY": "",
            "CLEARML_API_ACCESS_KEY": "",
            "CLEARML_API_SECRET_KEY": "",
            "CLEARML_BASE_URL": "",
            "CLEARML_RESULT_BUCKET": "",
            "CLEARML_GIT_USER": "",
            "CLEARML_GIT_PASSWORD": "",
        }
    },
}


@attr.s
class RunpodDriver(CloudDriver):
    """Runpod Driver"""

    runpod_api_key = attr.ib(validator=instance_of(str), default="")
    default_image = attr.ib(validator=instance_of(str), default=DEFAULT_DOCKER_IMAGE)

    @classmethod
    def from_config(cls, config):
        obj = super().from_config(config)
        obj.runpod_api_key = config["hyper_params"].get("runpod_api_key")
        obj.extra_env_vars = config["configurations"].get("extra_env_vars", {})
        obj.default_image = config["hyper_params"].get("default_docker_image", DEFAULT_DOCKER_IMAGE)
        return obj

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        self.tags = parse_tags(self.tags)

    def spin_up_worker(self, resource_conf, worker_prefix, queue_name, task_id):
        # Set API key
        runpod.api_key = self.runpod_api_key
        image = (self.default_image if "image" not in resource_conf else resource_conf["image"])

        if "spot" not in resource_conf or not resource_conf["spot"]:
            response = runpod.create_pod(
                name=f"{worker_prefix}-{queue_name}",
                cloud_type=resource_conf["cloud"],
                image_name=image,
                gpu_type_id=resource_conf["gpu_id"],
                gpu_count=resource_conf["gpus"],
                volume_in_gb=resource_conf["disk_volume"],
                container_disk_in_gb=resource_conf["disk_container"],
                min_vcpu_count=resource_conf["cpus"],
                min_memory_in_gb=resource_conf["memory"],
                docker_args="",
                ports=resource_conf["ports"] if "ports" in resource_conf else None,
                volume_mount_path="/workspace",
                env={
                    "DYNAMIC_INSTANCE_ID": "$RUNPOD_POD_ID",
                    "CLEARML_WORKER_ID": worker_prefix + ":$RUNPOD_POD_ID",
                    "QUEUE": queue_name,
                    **self.extra_env_vars,
                },
            )
        else:
            response = runpod.create_spot_pod(
                name=f"{worker_prefix}-{queue_name}",
                template_id=resource_conf["template_id"],
                cloud_type=resource_conf["cloud"],
                gpu_type_id=resource_conf["gpu_id"],
                gpu_count=resource_conf["gpus"],
                volume_in_gb=resource_conf["disk_volume"],
                container_disk_in_gb=resource_conf["disk_container"],
                min_vcpu_count=resource_conf["cpus"],
                min_memory_in_gb=resource_conf["memory"],
                ports=resource_conf["ports"] if "ports" in resource_conf else None,
                volume_mount_path="/workspace",
                env={
                    "DYNAMIC_INSTANCE_ID": "$RUNPOD_POD_ID",
                    "CLEARML_WORKER_ID": worker_prefix + ":$RUNPOD_POD_ID",
                    "QUEUE": queue_name,
                    **self.extra_env_vars,
                },
                bid=resource_conf["bid"],
            )

        print("Started", response)
        return response["id"]

    def spin_down_worker(self, instance_id):
        # Set API key
        runpod.api_key = self.runpod_api_key

        response = runpod.terminate_pod(instance_id)
        print(f"Spinning down {instance_id}", response)

    def creds(self):
        creds = {
            "runpod_api_key": self.runpod_api_key,
        }
        return creds

    def instance_id_command(self):
        return "echo $RUNPOD_POD_ID"

    def instance_type_key(self):
        return "instance_type"

    def kind(self):
        return "RunPod"

    def console_log(self, instance_id):
        return "Not supported."


def main():
    parser = ArgumentParser()
    parser.add_argument(
        "--remote",
        help="Run the autoscaler as a service, launch on the `services` queue",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--config-file",
        help="Configuration file name",
        type=Path,
        default=Path("runpod_autoscaler.yaml"),
    )
    args = parser.parse_args()
    conf = default_config

    # Connecting ClearML with the current process,
    # from here on everything is logged automatically
    task = Task.init(
        project_name="DevOps Services",
        task_name="RunPod Auto-Scaler",
        task_type=Task.TaskTypes.service,
    )
    task.connect(conf["hyper_params"])
    configurations = conf["configurations"]
    configurations.update(
        json.loads(task.get_configuration_object(name="General") or "{}")
    )
    task.set_configuration_object(
        name="General", config_text=json.dumps(configurations, indent=2)
    )

    print(
        "Running RunPod auto-scaler as a service\nExecution log {}".format(
            task.get_output_log_web_page()
        )
    )

    if args.remote:
        # if we are running remotely enqueue this run, and leave the process
        # the clearml-agent services will pick it up and execute it for us.
        task.execute_remotely(queue_name="services")

    driver = RunpodDriver.from_config(conf)
    conf = ScalerConfig.from_config(conf)
    autoscaler = AutoScaler(conf, driver)
    autoscaler.start()


if __name__ == "__main__":
    main()
