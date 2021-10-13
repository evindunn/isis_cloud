from os import chdir, getcwd, getenv
from os.path import exists as path_exists, join as path_join
from os import remove
from subprocess import run as sp_run, PIPE, DEVNULL
from errno import ENOENT as FILE_NOT_FOUND
from uuid import uuid4

from flask import request, jsonify

from .._config import ISISServerConfig


def _serialize_command_args(arg_dict):
    args = list()
    listfiles = list()
    for k, v in arg_dict.items():
        # If the argument is a list, isis wants a "listfile"
        if isinstance(v, list):
            list_file = "{}.lis".format(uuid4())
            with open(list_file, 'w') as f:
                for item in v:
                    print(item, file=f)
            v = list_file
            listfiles.append(list_file)

        args.append("{}={}".format(k, str(v)))

    # Return the listfiles too so we can clean them up
    return args, listfiles


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

        command_args, listfiles = _serialize_command_args(req["args"])

        status = 200
        response = {"message": "Command executed successfully"}
        proc = sp_run([command, *command_args], stdout=DEVNULL, stderr=PIPE)

        if not proc.returncode == 0:
            status = 500
            response["message"] = proc.stderr.decode("utf-8")

        # Auto-cleanup listfiles
        for file in listfiles:
            remove(file)

        return jsonify(response), status

    finally:
        chdir(orig_dir)
