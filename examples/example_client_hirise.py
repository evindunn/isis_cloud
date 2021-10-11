#!/usr/bin/env python3
from concurrent.futures import ThreadPoolExecutor
from sys import path as sys_path
from os.path import dirname, basename, join as path_join, exists as path_exists, realpath, splitext
from os import makedirs
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


data_dir = "/data/disk/hirise_jezero"
output_dir = path_join(data_dir, "output")

for directory in [data_dir, output_dir]:
    if not path_exists(directory):
        makedirs(directory)

input_urls = [
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/HiRISE/EDR/ESP/ORB_011600_011699/ESP_011630_1985/ESP_011630_1985_RED5_0.IMG",
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/HiRISE/EDR/ESP/ORB_011600_011699/ESP_011630_1985/ESP_011630_1985_RED5_1.IMG",
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/HiRISE/EDR/ESP/ORB_011600_011699/ESP_011630_1985/ESP_011630_1985_BG13_0.IMG",
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/HiRISE/EDR/ESP/ORB_011600_011699/ESP_011630_1985/ESP_011630_1985_BG13_1.IMG",
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/HiRISE/EDR/ESP/ORB_011600_011699/ESP_011630_1985/ESP_011630_1985_IR11_0.IMG",
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/HiRISE/EDR/ESP/ORB_011600_011699/ESP_011630_1985/ESP_011630_1985_IR11_1.IMG",
]
input_files = list()
output_file_prefix = "ESP_011630_1985"

client = ISISClient("http://127.0.0.1:8080/api/v1")

with ThreadPoolExecutor() as pool:
    for input_url in input_urls:
        input_url_parsed = urlparse(input_url)
        download_file = path_join(data_dir, basename(input_url_parsed.path))

        if not path_exists(download_file):
            pool.submit(ISISClient.fetch, input_url, download_file)

        input_files.append(download_file)

reduce_size = (512, 30000)
summing_mode = 2
red = input_files[0:2]
blue_green = input_files[2:4]
ir = input_files[4:6]

for file_lst in [red, blue_green, ir]:
    for idx, file in enumerate(file_lst):
        file_basename = basename(file)
        file_basename, _ = splitext(file_basename)

        cub_file = "{}.cub".format(file_basename)
        hi2isis = client.command("hi2isis")
        hi2isis.add_file_arg("from", file)
        hi2isis.add_arg("to", cub_file)
        hi2isis.send()

        client.rm(basename(file))

        spiceinit = client.command("spiceinit")
        spiceinit.add_arg("from", cub_file)
        spiceinit.add_arg("web", "true")
        spiceinit.send()

        cal_file = "{}.cal.cub".format(file_basename)
        hical = client.command("hical")
        hical.add_arg("from", cub_file)
        hical.add_arg("to", cal_file)
        hical.send()

        client.rm(cub_file)

        reduced_file = "{}.reduced.cub".format(file_basename)

        reduce = client.command("reduce")
        reduce.add_arg("from", cal_file)
        reduce.add_arg("mode", "total")
        reduce.add_arg("ons", reduce_size[0])
        reduce.add_arg("onl", reduce_size[1])
        reduce.add_arg("to", reduced_file)
        reduce.send()

        client.rm(cal_file)

        editlab = client.command("editlab")
        editlab.add_arg("from", reduced_file)
        editlab.add_arg("grpname", "Instrument")
        editlab.add_arg("keyword", "Summing")
        editlab.add_arg("value", summing_mode)
        editlab.send()

        file_lst[idx] = reduced_file

for stitched_file, file_lst in [("red.cub", red), ("bg.cub", blue_green), ("ir.cub", ir)]:
    histitch = client.command("histitch")
    histitch.add_arg("from1", file_lst[0])
    histitch.add_arg("from2", file_lst[1])
    histitch.add_arg("to", stitched_file)
    histitch.send()

    for file in file_lst:
        client.rm(file)

for stitched_file in ["bg.cub", "ir.cub"]:
    cnet_file = "{}.cnet".format(stitched_file)

    hijitreg = client.command("hijitreg")
    hijitreg.add_arg("from", stitched_file)
    hijitreg.add_arg("match", "red.cub")
    hijitreg.add_arg("cnetfile", cnet_file)
    hijitreg.send()

    slithered_file = stitched_file.replace(".cub", ".slither.cub")

    slither = client.command("slither")
    slither.add_arg("from", stitched_file)
    slither.add_arg("control", cnet_file)
    slither.add_arg("to", slithered_file)
    slither.send()

    client.rm(stitched_file)
    client.rm(cnet_file)

stacked_file = "{}.cub".format(output_file_prefix)

hicubeit = client.command("hicubeit")
hicubeit.add_arg("red", "red.cub")
hicubeit.add_arg("ir", "ir.slither.cub")
hicubeit.add_arg("bg", "bg.slither.cub")
hicubeit.add_arg("to", stacked_file)
hicubeit.send()

for file in ["red.cub", "ir.slither.cub", "bg.slither.cub"]:
    client.rm(file)

mapped_file = "{}.mapped.cub".format(output_file_prefix)
proj_file = "simple-cylindrical.map"

maptemplate = client.command("maptemplate")
maptemplate.add_arg("projection", "SimpleCylindrical")
maptemplate.add_arg("clon", 0.0)
maptemplate.add_arg("map", proj_file)
maptemplate.send()

cam2map = client.command("cam2map")
cam2map.add_arg("from", stacked_file)
cam2map.add_arg("map", proj_file)
cam2map.add_arg("to", mapped_file)
cam2map.send()

client.rm(stacked_file)
client.rm(proj_file)

color_mos_file = "{}.mos.cub".format(output_file_prefix)

hicolormos = client.command("hicolormos")
hicolormos.add_arg("from1", mapped_file)
hicolormos.add_arg("to", color_mos_file)
hicolormos.send()

client.rm(mapped_file)

final_outfile = "{}.tif".format(output_file_prefix)

isis2std = client.command("isis2std")
isis2std.add_arg("mode", "rgb")
isis2std.add_arg("red", "{}+1".format(color_mos_file))
isis2std.add_arg("green", "{}+2".format(color_mos_file))
isis2std.add_arg("blue", "{}+3".format(color_mos_file))
isis2std.add_arg("to", final_outfile)
isis2std.add_arg("format", "tif")
isis2std.send()

client.rm(color_mos_file)

client.download(final_outfile, path_join(output_dir, final_outfile))

client.rm(final_outfile)
