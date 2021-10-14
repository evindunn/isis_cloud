#!/usr/bin/env python3

from sys import path as sys_path
from os.path import dirname, basename, realpath
from sys import stderr
from logging import basicConfig as logConfig, getLogger, DEBUG, ERROR
from urllib.parse import urlparse

pkg_dir = dirname(dirname(realpath(__file__)))
sys_path.insert(0, pkg_dir)

from isis_cloud.client import ISISClient

logConfig(
    level=DEBUG,
    datefmt="%F %T",
    format="[%(asctime)s][%(levelname)s][%(name)s] %(message)s"
)
getLogger("urllib3.connectionpool").setLevel(ERROR)


client = ISISClient("http://127.0.0.1:8080/api/v1")
input_url = "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/CTX/mrox_2578/data/J03_045994_1986_XN_18N282W.IMG"

input_url_parsed = urlparse(input_url)
input_file = basename(input_url_parsed.path)
output_file = input_file.replace(".IMG", ".tif")

try:
    ISISClient.fetch(
        input_url,
        input_file
    )

    mroctx2isis = client.program("mroctx2isis")
    mroctx2isis.add_file_arg(
        "from",
        input_file
    )
    mroctx2isis.add_arg("to", "mro.cub")
    mroctx2isis.send()

    spiceinit = client.program("spiceinit")
    spiceinit.add_arg("from", "mro.cub")
    spiceinit.add_arg("web", "true")
    spiceinit.send()

    ctxcal = client.program("ctxcal")
    ctxcal.add_arg("from", "mro.cub")
    ctxcal.add_arg("to", "ctxcal.cub")
    ctxcal.send()

    ctxevenodd = client.program("ctxevenodd")
    ctxevenodd.add_arg("from", "ctxcal.cub")
    ctxevenodd.add_arg("to", "ctxevenodd.cub")
    ctxevenodd.send()

    maptemplate = client.program("maptemplate")
    maptemplate.add_arg("projection", "Equirectangular")
    maptemplate.add_arg("map", "equirectangular.map")
    maptemplate.add_arg("clon", "0.0")
    maptemplate.add_arg("clat", "0.0")
    maptemplate.send()

    cam2map = client.program("cam2map")
    cam2map.add_arg("from", "ctxevenodd.cub")
    cam2map.add_arg("map", "equirectangular.map")
    cam2map.add_arg("to", "cam2map.cub")
    cam2map.send()

    isis2std = client.program("isis2std")
    isis2std.add_arg("from", "cam2map.cub")
    isis2std.add_arg("to", output_file)
    isis2std.add_arg("format", "tiff")
    isis2std.send()

    client.download(output_file, output_file)

except Exception as e:
    print(e, file=stderr)
    exit(1)
