import logging
from os import chdir, getcwd, getenv
from os.path import exists as path_exists, join as path_join, basename
from os import remove
from subprocess import run as sp_run, PIPE, DEVNULL
from urllib.request import urlretrieve
from uuid import uuid4
from logging import getLogger
from tempfile import NamedTemporaryFile

from flask import request, jsonify

from .._config import ISISServerConfig

logger = getLogger("ISIS")


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
    temp_files = list()
    listfiles = list()

    orig_dir = getcwd()
    chdir(ISISServerConfig.work_dir())

    try:
        body = request.get_json()

        # Only allow executables in the conda bin
        command = path_join(
            getenv("ISISROOT"),
            "bin",
            body["program"].strip("/")
        )

        if not path_exists(command):
            return jsonify({"message": "Command not found"}), 404

        remote_files = body.pop("remotes", [])

        # Download any arguments that are tagged as remote files
        for arg_key in remote_files:
            if arg_key not in body["args"].keys():
                return jsonify({
                    "message": "remote '{}' not found in args".format(arg_key)
                }), 400

            url = body["args"][arg_key]
            dl_file = NamedTemporaryFile('r+', dir=getcwd())
            urlretrieve(url, dl_file.name)
            temp_files.append(dl_file)
            body["args"][arg_key] = dl_file.name

        command_args, listfiles = _serialize_command_args(body["args"])

        status = 200
        response = {"message": "Command executed successfully"}
        proc = sp_run([command, *command_args], stdout=DEVNULL, stderr=PIPE)

        if not proc.returncode == 0:
            status = 500
            stderr = proc.stderr.decode("utf-8")
            response["message"] = stderr

            err_msg = "{} failed\n{}".format(
                ' '.join([basename(command), *command_args]),
                stderr
            )
            logging.error(err_msg)

        return jsonify(response), status

    finally:
        # Auto-cleanup listfiles
        [remove(f) for f in listfiles if path_exists(f)]

        # Clean up temp files
        [f.close() for f in temp_files]

        # Change back to original working directory
        chdir(orig_dir)
