#!/usr/bin/env python3

from typing import List, Any

from yaml import load as yaml_load
from sys import path as sys_path
from os.path import dirname, basename, realpath, join as path_join
from logging import basicConfig as logConfig, getLogger, DEBUG, ERROR
from uuid import uuid4

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

pkg_dir = dirname(dirname(realpath(__file__)))
sys_path.insert(0, pkg_dir)

from isis_cloud.client import ISISClient


def parse_pipeline_arg(arg: Any, inputs: List[str]) -> str:
    parsed_arg = str(arg)
    for input_idx in range(len(inputs)):
        input_val = inputs[input_idx]
        parsed_arg = parsed_arg.replace("$uuid()", str(uuid4()))
        parsed_arg = parsed_arg.replace("${}".format(input_idx + 1), input_val)
    return parsed_arg


def parse_pipeline_args(args: dict, inputs: List[str]) -> dict:
    new_args = {**args}
    for arg_name, arg_val in args.items():
        new_args[arg_name] = parse_pipeline_arg(arg_val, inputs)
    return new_args


def parse_pipeline_outputs(args: dict, outputs: List[str]) -> List[str]:
    new_outputs = [*outputs]
    for arg_name, arg_val in args.items():
        for out_idx in range(len(new_outputs)):
            if new_outputs[out_idx] == arg_name:
                new_outputs[out_idx] = arg_val
    return new_outputs


PIPELINE_YAML = path_join(dirname(__file__), "pipeline.yml")
client = ISISClient("http://127.0.0.1:8080/api/v1")

logConfig(
    level=DEBUG,
    datefmt="%F %T",
    format="[%(asctime)s][%(levelname)s][%(name)s] %(message)s"
)
getLogger("urllib3.connectionpool").setLevel(ERROR)

with open(PIPELINE_YAML) as f:
    pipeline_yml = yaml_load(f, Loader=Loader)

commands = pipeline_yml["pipeline"]
input_files = pipeline_yml["input_files"]

for input_file in input_files:
    current_inputs = [input_file]

    for command in commands:
        if "download" in command.keys():
            dl_file = parse_pipeline_arg(command["download"], current_inputs)
            client.download(dl_file, basename(dl_file))
            continue

        cmd_str = command["cmd"]
        parsed_args = parse_pipeline_args(command["args"], current_inputs)
        outputs = parse_pipeline_outputs(parsed_args, command["outputs"])

        cmd = client.program(cmd_str)
        for arg_name, arg_val in parsed_args.items():
            cmd.add_arg(arg_name, arg_val, is_remote=arg_val.startswith("http"))
        cmd.send()

        current_inputs = outputs
