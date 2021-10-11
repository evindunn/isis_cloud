#!/usr/bin/env python3

from sys import path as sys_path
from os.path import dirname, realpath
from os import environ

# This is where I keep it, different for each server
environ["ISISDATA"] = "/data/disk/isisdata"

pkg_dir = dirname(dirname(realpath(__file__)))
sys_path.insert(0, pkg_dir)

from isis_cloud.server import ISISServer

server = ISISServer()
server.run(8080, debug=True)
