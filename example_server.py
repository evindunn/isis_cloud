#!/usr/bin/env python3
from logging import getLogger

import connexion

from sys import path as syspath
from os.path import dirname
from os import environ

# This is where I keep it, different for each server
if "ISISDATA" not in environ.keys():
    environ["ISISDATA"] = "/data/disk/isisdata"

syspath.insert(0, dirname(__file__))

isis_server = connexion.FlaskApp(
    __name__,
    specification_dir=".",
    options={"swagger_url": "/docs"}
)

isis_server.add_api("openapi.yml")
isis_server.run(port=8080)
