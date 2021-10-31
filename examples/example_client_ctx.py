#!/usr/bin/env python3

from sys import path as sys_path
from os.path import dirname, basename, realpath
from sys import stderr
from logging import basicConfig as logConfig, getLogger, DEBUG, ERROR
from urllib.parse import urlparse
from os.path import join as path_join
from tempfile import TemporaryDirectory
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

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
input_urls = [
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/CTX/mrox_0047/data/P03_002387_1987_XI_18N282W.IMG",
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/CTX/mrox_0589/data/P19_008650_1987_XI_18N282W.IMG",
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/CTX/mrox_1895/data/D16_033651_1987_XN_18N281W.IMG",
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/CTX/mrox_2013/data/D22_035629_1987_XN_18N282W.IMG",
]
output_file = "jezero.tif"


def get_random_cub_filename():
    return "{}.cub".format(uuid4())


def generate_lev2_file(cub_file):
    spiceinit = client.program("spiceinit")
    spiceinit.add_arg("from", cub_file)
    spiceinit.add_arg("web", "true")
    spiceinit.send()

    lev1_file = get_random_cub_filename()

    ctxcal = client.program("ctxcal")
    ctxcal.add_arg("from", cub_file)
    ctxcal.add_arg("to", lev1_file)
    ctxcal.send()

    eo_file = get_random_cub_filename()

    ctxevenodd = client.program("ctxevenodd")
    ctxevenodd.add_arg("from", lev1_file)
    ctxevenodd.add_arg("to", eo_file)
    ctxevenodd.send()

    norm_file = get_random_cub_filename()

    cubenorm = client.program("cubenorm")
    cubenorm.add_arg("from", eo_file)
    cubenorm.add_arg("to", norm_file)
    cubenorm.send()

    lev2_file = get_random_cub_filename()

    cam2map = client.program("cam2map")
    cam2map.add_arg("from", norm_file)
    cam2map.add_arg("map", map_file)
    cam2map.add_arg("pixres", "MPP")
    cam2map.add_arg("resolution", 5)     # Original on these files is >5, <6
    cam2map.add_arg("to", lev2_file)
    cam2map.send()

    [client.delete(file) for file in [lev1_file, eo_file, norm_file]]

    return lev2_file


try:
    with TemporaryDirectory() as temp_dir:
        dl_files = list()

        with ThreadPoolExecutor() as pool:
            for input_url in input_urls:
                input_url_parsed = urlparse(input_url)
                dl_file = path_join(
                    temp_dir,
                    basename(input_url_parsed.path)
                )

                pool.submit(
                    ISISClient.fetch,
                    input_url,
                    dl_file
                )
                dl_files.append(dl_file)

        cub_files = list()

        with ThreadPoolExecutor() as pool:
            for input_file in dl_files:
                cub_file = get_random_cub_filename()
                mroctx2isis = client.program("mroctx2isis")
                mroctx2isis.add_file_arg(
                    "from",
                    input_file
                )
                mroctx2isis.add_arg("to", cub_file)
                pool.submit(mroctx2isis.send)
                cub_files.append(cub_file)

    [client.delete(basename(f)) for f in dl_files]

    map_file = "{}.map".format(uuid4())
    maptemplate = client.program("maptemplate")
    maptemplate.add_arg("projection", "Equirectangular")
    maptemplate.add_arg("map", map_file)
    maptemplate.add_arg("clon", "0.0")
    maptemplate.add_arg("clat", "0.0")
    maptemplate.send()

    threads = list()

    with ThreadPoolExecutor() as pool:
        for cub_file in cub_files:
            thread = pool.submit(
                generate_lev2_file,
                cub_file
            )
            threads.append(thread)

    [client.delete(f) for f in [map_file, *cub_files]]

    lev2_cubs = [t.result() for t in threads]

    equalized_cubs = [get_random_cub_filename() for _ in range(len(lev2_cubs))]

    equalizer = client.program("equalizer")
    equalizer.add_arg("fromlist", lev2_cubs)
    equalizer.add_arg("tolist", equalized_cubs)
    equalizer.add_arg("holdlist", [lev2_cubs[0]])
    equalizer.send()

    [client.delete(f) for f in lev2_cubs]

    noseam_mos = get_random_cub_filename()

    noseam = client.program("noseam")
    noseam.add_arg("fromlist", equalized_cubs)
    noseam.add_arg("to", noseam_mos)
    noseam.add_arg("samples", 3)
    noseam.add_arg("lines", 3)
    noseam.send()

    [client.delete(f) for f in equalized_cubs]

    isis2std = client.program("isis2std")
    isis2std.add_arg("from", noseam_mos)
    isis2std.add_arg("to", output_file)
    isis2std.add_arg("format", "tiff")
    isis2std.send()

    client.delete(noseam_mos)

    client.download(output_file, output_file)
    client.delete(output_file)

except Exception as e:
    print(e, file=stderr)
    exit(1)
