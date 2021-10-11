#!/usr/bin/env python3
from concurrent.futures import ThreadPoolExecutor
from sys import path as sys_path, stderr
from os.path import dirname, basename, join as path_join, exists as path_exists, realpath, splitext
from os import makedirs
from logging import basicConfig as logConfig, getLogger, DEBUG, ERROR
from urllib.parse import urlparse
from json import load as json_load
from re import sub as re_sub, search as re_search

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
client = ISISClient("http://127.0.0.1:8080/api/v1")

for directory in [data_dir, output_dir]:
    if not path_exists(directory):
        makedirs(directory)

with open("hirise_jezero.json") as f:
    input_urls = json_load(f)

input_files = list()

with ThreadPoolExecutor() as pool:
    for input_url in input_urls:
        input_url_parsed = urlparse(input_url)
        download_file = path_join(data_dir, basename(input_url_parsed.path))

        if not path_exists(download_file):
            pool.submit(ISISClient.fetch, input_url, download_file)

        input_files.append(download_file)


mosaic_inputs = list()
files_processed = list()
stack_regex = r"(RED|IR|BG)\d{1,2}_\d.IMG"

# TODO: Do them all
for input_file in input_files:
    if input_file in files_processed:
        continue

    file_basename = basename(input_file)
    file_basename, _ = splitext(file_basename)
    as_cub = "{}.cub".format(file_basename)

    files_processed.append(input_file)
    stack = dict()

    if "RED" in input_file:
        stack["RED"] = input_file
    elif "BG" in input_file:
        stack["BG"] = input_file
    else:
        stack["IR"] = input_file

    stack_search = re_sub(stack_regex, "", input_file)

    for file in input_files:
        if re_search(stack_regex, file) is not None:
            files_processed.append(file)
            if "RED" in file:
                stack["RED"] = file
            elif "BG" in file:
                stack["BG"] = file
            else:
                stack["IR"] = file

    try:
        for stack_file in stack.values():

            hi2isis = client.command("hi2isis")
            hi2isis.add_file_arg("from", stack_file)
            hi2isis.add_arg("to", as_cub)
            hi2isis.send()

            client.rm(basename(stack_file))

            spiceinit = client.command("spiceinit")
            spiceinit.add_arg("from", as_cub)
            spiceinit.add_arg("web", "true")
            spiceinit.send()

            as_calibrated = "{}.cal.cub".format(file_basename)

            hical = client.command("hical")
            hical.add_arg("from", as_cub)
            hical.add_arg("to", as_calibrated)
            hical.send()

            client.rm(basename(as_cub))

            map_file = "equirectangular.map"

            maptemplate = client.command("maptemplate")
            maptemplate.add_arg("projection", "Equirectangular")
            maptemplate.add_arg("map", map_file)
            maptemplate.add_arg("clon", "0.0")
            maptemplate.add_arg("clat", "0.0")
            maptemplate.send()

            as_mapped = "{}.mapped.cub".format(file_basename)

            cam2map = client.command("cam2map")
            cam2map.add_arg("from", as_calibrated)
            cam2map.add_arg("map", map_file)
            cam2map.add_arg("to", as_mapped)
            cam2map.send()

            client.rm(map_file)
            client.rm(basename(as_calibrated))

        stacked_cub = "{}.stack.cub".format(file_basename)
        hicubit = client.command("hicubeit")
        hicubit.add_arg("red", stack["RED"])
        hicubit.add_arg("ir", stack["IR"])
        hicubit.add_arg("bg", stack["BG"])
        hicubit.add_arg("to", stacked_cub)

        as_png = stacked_cub.replace(".cub", ".png")

        isis2std = client.command("isis2std")
        isis2std.add_arg("from", stacked_cub)
        isis2std.add_arg("to", as_png)

        client.rm(basename(stacked_cub))

        client.cp(stacked_cub, path_join(output_dir, as_png))

    except Exception as e:
        print(e, file=stderr)
        exit(1)
