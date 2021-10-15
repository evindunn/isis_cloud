from flask import request, send_from_directory
from os.path import exists as path_exists, join as path_join
from os import makedirs, remove
from pvl import load as pvl_load

from .._config import ISISServerConfig


def upload_file():
    if not path_exists(ISISServerConfig.work_dir()):
        makedirs(ISISServerConfig.work_dir(), mode=0o700)

    for file_name in request.files.keys():
        file_path = path_join(ISISServerConfig.work_dir(), file_name)
        request.files[file_name].save(file_path)


def retrieve_file(file_name):
    file_path = path_join(ISISServerConfig.work_dir(), file_name.strip("/"))
    if not path_exists(file_path):
        return {"message": "File not found"}, 404

    return send_from_directory(ISISServerConfig.work_dir(), file_name)


def retrieve_file_label(file_name):
    file_path = path_join(ISISServerConfig.work_dir(), file_name.strip("/"))
    if not path_exists(file_path):
        return {"message": "File not found"}, 404

    try:
        return pvl_load(file_path)
    except:
        return {"message": "Invalid cube label for '{}'".format(file_name)}, 500

def delete_file(file_name):
    file_path = path_join(ISISServerConfig.work_dir(), file_name.strip("/"))
    if not path_exists(file_path):
        return {"message": "File not found"}, 404

    remove(file_path)
