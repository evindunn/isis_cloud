from os import chdir, getcwd
from os.path import exists as path_exists
from os import remove

from flask import request
from kalasiris import kalasiris

from isis_cloud._config import ISISServerConfig


def run_isis():
    orig_dir = getcwd()
    chdir(ISISServerConfig.WORK_DIR)

    try:
        req = request.json

        command = req.pop("command")
        command_args = req["args"]

        isis_func = getattr(kalasiris, command)

        for file in ["print.prt", "errors.prt"]:
            if path_exists(file):
                remove(file)

        response = ({"message": "Command executed successfully"}, 200)
        try:
            isis_func(**command_args)

        except Exception as e:
            try:
                kalasiris.errors(from_="print.prt", to="errors.prt")
                with open("errors.prt") as f:
                    error = f.read()
            except:
                error = str(e)
                response = ({"message": error}, 500)

        return response

    finally:
        chdir(orig_dir)
