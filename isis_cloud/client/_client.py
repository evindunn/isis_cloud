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

    def command(self, command: str):
        return ISISRequest(self._server_addr, command)

    def cp(self, remote_path, local_path):
        return ISISClient.fetch(self._file_url(remote_path), local_path)

    def rm(self, remote_path):
        remote_url = self._file_url(remote_path)
        ISISClient.logger.debug("Deleting {}...".format(remote_url))
        r = requests.delete(remote_url)
        _catch_err(r)
        ISISClient.logger.debug("{} deleted successfully".format(remote_url))

    @staticmethod
    def fetch(remote_url, download_path):
        ISISClient.logger.debug("Downloading {}...".format(remote_url))
        start_time = time()

        download_file = open(download_path, 'wb')
        response = requests.get(remote_url, stream=True)

        _catch_err(response)

        with response, download_file:
            for chunk in response.iter_content(chunk_size=ISISClient._DL_CHUNK_SIZE):
                download_file.write(chunk)

        log_msg = "{} downloaded to {} (took {:.1f}s)".format(
            remote_url,
            download_path,
            time() - start_time
        )
        ISISClient.logger.debug(log_msg)


class ISISRequest:
    def __init__(self, server_url: str, command: str):
        self._server_url = server_url
        self._command = command
        self._args = dict()
        self._files = dict()
        self._logger = getLogger(command)

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

        r = requests.post(
            "/".join([self._server_url, "isis"]),
            json={
                "command": self._command,
                "args": command_args
            }
        )
        _catch_err(r)

        self._logger.debug("Took {:.1f}s".format(time() - start_time))
