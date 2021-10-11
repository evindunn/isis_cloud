#!/usr/bin/env python3

from sys import path as sys_path
from os.path import dirname, realpath
from logging import basicConfig as log_config, INFO

pkg_dir = dirname(realpath(__file__))
sys_path.insert(0, pkg_dir)

from isis_cloud.server import ISISServer

log_config(
    format="[%(name)s][%(levelname)s] %(message)s",
    level=INFO
)

app = ISISServer()

if __name__ == "__main__":
    app.run(8080, debug=True)
