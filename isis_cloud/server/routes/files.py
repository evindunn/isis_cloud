from flask import request, send_from_directory
from os.path import exists as path_exists, join as path_join
from os import makedirs

from ..._config import ISISServerConfig


def upload_file():
    if not path_exists(ISISServerConfig.WORK_DIR):
        makedirs(ISISServerConfig.WORK_DIR, mode=0o700)

    for file_name in request.files.keys():
        file_path = path_join(ISISServerConfig.WORK_DIR, file_name)
        request.files[file_name].save(file_path)


def retrieve_file(file_name):
    file_path = path_join(ISISServerConfig.WORK_DIR, file_name)
    if not path_exists(file_path):
        return {"message": "File not found"}, 404

    return send_from_directory(ISISServerConfig.WORK_DIR, file_name)
