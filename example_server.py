#!/usr/bin/env python3

import connexion

from sys import path as syspath
from os.path import dirname
from os import environ

# This is where I keep it, different for each server
environ["ISISDATA"] = "/data/disk/isisdata"

syspath.insert(0, dirname(__file__))

app = connexion.FlaskApp(
    __name__,
    specification_dir=".",
    options={"swagger_url": "/docs"}
)
app.add_api("isis-server.yml")
app.run(port=8080, debug=True)
