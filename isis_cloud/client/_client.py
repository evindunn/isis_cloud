import json
from contextlib import closing
from urllib.error import URLError, HTTPError
from urllib.request import urlretrieve
from os.path import basename
from time import time

import requests
from urllib.parse import quote_plus as url_quote
from logging import getLogger


def _catch_err(req):
    if not req.ok:
        err = "Server responded with {}".format(req.status_code)

        if req.headers.get("content-type").startswith("application/json"):
            req_json = req.json()
            if "message" in req_json.keys():
                err = "Server responded with {}: {}".format(
                    req.status_code,
                    req_json["message"]
                )

        raise RuntimeError(err)


class ISISClient:
    logger = getLogger("ISISClient")

    # 64KiB
    _DL_CHUNK_SIZE = 64000

    def __init__(self, server_addr: str):
        self._server_addr = server_addr

    def _file_url(self, file_path):
        file_path = url_quote(file_path)
        return "/".join([self._server_addr, "files", file_path])

    def _label_url(self, file_path):
        return "/".join([self._file_url(file_path), "label"])

    def program(self, command: str):
        return ISISRequest(self._server_addr, command)

    def download(self, remote_path, local_path):
        return ISISClient.fetch(self._file_url(remote_path), local_path)

    def delete(self, remote_path):
        remote_url = self._file_url(remote_path)
        ISISClient.logger.debug("Deleting {}...".format(remote_url))
        r = requests.delete(remote_url)
        _catch_err(r)
        ISISClient.logger.debug("{} deleted successfully".format(remote_url))

    def label(self, remote_path):
        remote_url = self._label_url(remote_path)
        ISISClient.logger.debug("Retrieving label for {}...".format(remote_url))
        r = requests.get(remote_url)
        _catch_err(r)
        ISISClient.logger.debug("Label for {} retrieved successfully".format(remote_url))
        return r.json()

    @staticmethod
    def fetch(remote_url, download_path):
        ISISClient.logger.debug("Downloading {}...".format(remote_url))
        start_time = time()

        # urlretrieve can do both http & ftp
        try:
            urlretrieve(remote_url, download_path)
        except HTTPError as e:
            err_msg = "Server returned {}: {}".format(e.code, e.reason)
            raise RuntimeError(err_msg)
        except URLError as e:
            err_msg = "Server returned '{}'".format(e.reason)
            raise RuntimeError(err_msg)

        log_msg = "{} downloaded to {} (took {:.1f}s)".format(
            remote_url,
            download_path,
            time() - start_time
        )
        ISISClient.logger.debug(log_msg)


class ISISRequest:
    def __init__(self, server_url: str, program: str):
        self._server_url = server_url
        self._program = program
        self._args = dict()
        self._files = dict()
        self._logger = getLogger(program)

    def add_arg(self, arg_name, arg_value):
        self._args[arg_name] = arg_value
        return self

    def add_file_arg(self, arg_name, file_path):
        self._files[arg_name] = file_path
        return self

    def send(self):
        self._logger.debug("Starting...")
        start_time = time()

        file_uploads = dict()
        command_args = {**self._args}

        for arg_name, file_path in self._files.items():
            file_name = basename(file_path)
            file_uploads[file_name] = open(self._files[arg_name], 'rb')
            command_args[arg_name] = file_name

        if len(file_uploads.keys()) > 0:
            r = requests.post(
                "/".join([self._server_url, "files"]),
                files=file_uploads
            )
            _catch_err(r)

        cmd_req = {
            "program": self._program,
            "args": command_args
        }

        r = requests.post(
            "/".join([self._server_url, "isis"]),
            json=cmd_req
        )

        try:
            _catch_err(r)
        except RuntimeError as e:
            self._logger.error(json.dumps(cmd_req))
            raise e

        self._logger.debug("Took {:.1f}s".format(time() - start_time))
