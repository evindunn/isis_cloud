from os import chdir, getcwd, getenv
from os.path import exists as path_exists, join as path_join
from os import remove
from subprocess import run as sp_run, PIPE, DEVNULL
from errno import ENOENT as FILE_NOT_FOUND


from flask import request, jsonify

from .._config import ISISServerConfig


def _serialize_command_args(arg_dict):
    args = list()
    for k, v in arg_dict.items():
        if isinstance(v, list):
            list_file = "{}.lis".format(k)
            with open(list_file, 'w') as f:
                f.writelines(v)
            v = list_file

        args.append("{}={}".format(k, str(v)))
    return args


def run_isis():
    orig_dir = getcwd()
    chdir(ISISServerConfig.work_dir())

    try:
        req = request.get_json()

        # Only allow executables in the conda bin
        command = path_join(
            getenv("ISISROOT"),
            "bin",
            req["command"].strip("/")
        )

        if not path_exists(command):
            return jsonify({"message": "Command not found"}), 404

        command_args = _serialize_command_args(req["args"])

        status = 200
        response = {"message": "Command executed successfully"}
        proc = sp_run([command, *command_args], stdout=DEVNULL, stderr=PIPE)

        if not proc.returncode == 0:
            status = 500
            response["message"] = proc.stderr.decode("utf-8")

        return jsonify(response), status

    finally:
        chdir(orig_dir)
