#!/usr/bin/env python3
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from sys import path as sys_path
from os.path import dirname, basename, join as path_join, exists as path_exists, realpath, splitext
from os import makedirs, remove
from logging import basicConfig as logConfig, getLogger, DEBUG, ERROR
from tempfile import TemporaryDirectory
from urllib.parse import urlparse
from re import search as re_search

# Thanks to
# https://repository.si.edu/bitstream/handle/10088/19366/nasm_201048.pdf?sequence=1&isAllowed=y
# for this

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


class HiRISEChannel:
    def __init__(self, red_url, bg_url, ir_url):
        dl_files = list()

        with ThreadPoolExecutor() as pool:
            for url in [red_url, bg_url, ir_url]:
                url_parsed = urlparse(url)
                dl_file = path_join(data_dir, basename(url_parsed.path))
                dl_files.append(dl_file)
                if not path_exists(dl_file):
                    pool.submit(ISISClient.fetch, url, dl_file)

        self.red = dl_files[0]
        self.bg = dl_files[1]
        self.ir = dl_files[2]

    def process(self):
        self._convert_to_cubes()
        self._enlarge_bg_ir()
        self._fix_jitter()

    def _convert_to_cubes(self):
        threads = list()
        with ThreadPoolExecutor() as pool:
            for file in [self.red, self.bg, self.ir]:
                threads.append(
                    pool.submit(HiRISEChannel._convert_to_cube, file)
                )

        self.red = threads[0].result()
        self.bg = threads[1].result()
        self.ir = threads[2].result()

    def _enlarge_bg_ir(self):
        enlarge_size = [0, 0]

        with TemporaryDirectory() as tmp_dir:
            red_dl = path_join(tmp_dir, self.red)
            client.download(self.red, red_dl)
            red_lbl = ISISClient.parse_cube_label(red_dl)

            enlarge_size[0] = red_lbl["IsisCube"]["Core"]["Dimensions"]["Samples"]
            enlarge_size[1] = red_lbl["IsisCube"]["Core"]["Dimensions"]["Lines"]
            summing_mode = red_lbl["IsisCube"]["Instrument"]["Summing"]

        with ThreadPoolExecutor() as pool:
            threads = list()
            for file in [self.bg, self.ir]:
                thread = pool.submit(
                    HiRISEChannel._resize_ir_bg,
                    file,
                    enlarge_size,
                    summing_mode
                )
                threads.append(thread)

        self.bg = threads[0].result()
        self.ir = threads[1].result()

    def _fix_jitter(self):
        with ThreadPoolExecutor() as pool:
            threads = list()
            for file in [self.bg, self.ir]:
                thread = pool.submit(
                    HiRISEChannel._fix_jitter_single,
                    self.red,
                    file
                )
                threads.append(thread)

        self.bg = threads[0].result()
        self.ir = threads[1].result()

    @staticmethod
    def strip_ext(file_name):
        return basename(file_name).split(".")[0]

    @staticmethod
    def _convert_to_cube(hirise_img):
        hirise_basename = HiRISEChannel.strip_ext(hirise_img)

        cub_file = "{}.cub".format(hirise_basename)
        hi2isis = client.command("hi2isis")
        hi2isis.add_file_arg("from", hirise_img)
        hi2isis.add_arg("to", cub_file)
        hi2isis.send()

        client.rm(basename(hirise_img))

        spiceinit = client.command("spiceinit")
        spiceinit.add_arg("from", cub_file)
        # spiceinit.add_arg("web", "true")
        spiceinit.send()

        cal_file = "{}.cal.cub".format(hirise_basename)
        hical = client.command("hical")
        hical.add_arg("from", cub_file)
        hical.add_arg("to", cal_file)
        hical.send()

        client.rm(cub_file)

        return cal_file

    @staticmethod
    def _resize_ir_bg(color_cube, resize_dims, summing_mode):
        enlarged_file = "{}.enlarged.cub".format(HiRISEChannel.strip_ext(color_cube))

        enlarge = client.command("enlarge")
        enlarge.add_arg("from", color_cube)
        enlarge.add_arg("mode", "total")
        enlarge.add_arg("interp", "bilinear")
        enlarge.add_arg("ons", resize_dims[0])
        enlarge.add_arg("onl", resize_dims[1])
        enlarge.add_arg("to", enlarged_file)
        enlarge.send()

        editlab = client.command("editlab")
        editlab.add_arg("from", enlarged_file)
        editlab.add_arg("grpname", "Instrument")
        editlab.add_arg("keyword", "Summing")
        editlab.add_arg("value", summing_mode)
        editlab.send()

        client.rm(color_cube)
        return enlarged_file

    @staticmethod
    def _fix_jitter_single(red, other_color):
        other_color_basename = HiRISEChannel.strip_ext(other_color)
        cnet_file = "{}.cnet".format(other_color_basename)
        slithered_file = "{}.slither.cub".format(other_color_basename)

        hijitreg = client.command("hijitreg")
        hijitreg.add_arg("from", other_color)
        hijitreg.add_arg("match", red)
        hijitreg.add_arg("cnetfile", cnet_file)
        hijitreg.send()

        slither = client.command("slither")
        slither.add_arg("from", other_color)
        slither.add_arg("control", cnet_file)
        slither.add_arg("to", slithered_file)
        slither.send()

        client.rm(cnet_file)
        client.rm(other_color)

        return slithered_file


for directory in [data_dir, output_dir]:
    if not path_exists(directory):
        makedirs(directory)

ir10_red4_bg12_0 = HiRISEChannel(
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/HiRISE/EDR/PSP/ORB_008800_008899/PSP_008831_1455/PSP_008831_1455_RED4_0.IMG",
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/HiRISE/EDR/PSP/ORB_008800_008899/PSP_008831_1455/PSP_008831_1455_BG12_0.IMG",
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/HiRISE/EDR/PSP/ORB_008800_008899/PSP_008831_1455/PSP_008831_1455_IR10_0.IMG",
)

ir10_red4_bg12_1 = HiRISEChannel(
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/HiRISE/EDR/PSP/ORB_008800_008899/PSP_008831_1455/PSP_008831_1455_RED4_1.IMG",
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/HiRISE/EDR/PSP/ORB_008800_008899/PSP_008831_1455/PSP_008831_1455_BG12_1.IMG",
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/HiRISE/EDR/PSP/ORB_008800_008899/PSP_008831_1455/PSP_008831_1455_IR10_1.IMG",
)

ir11_red5_bg13_0 = HiRISEChannel(
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/HiRISE/EDR/PSP/ORB_008800_008899/PSP_008831_1455/PSP_008831_1455_RED5_0.IMG",
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/HiRISE/EDR/PSP/ORB_008800_008899/PSP_008831_1455/PSP_008831_1455_BG13_0.IMG",
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/HiRISE/EDR/PSP/ORB_008800_008899/PSP_008831_1455/PSP_008831_1455_IR11_0.IMG",
)

ir11_red5_bg13_1 = HiRISEChannel(
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/HiRISE/EDR/PSP/ORB_008800_008899/PSP_008831_1455/PSP_008831_1455_RED5_1.IMG",
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/HiRISE/EDR/PSP/ORB_008800_008899/PSP_008831_1455/PSP_008831_1455_BG13_1.IMG",
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/HiRISE/EDR/PSP/ORB_008800_008899/PSP_008831_1455/PSP_008831_1455_IR11_1.IMG",
)

# ThreadPool inception
with ThreadPoolExecutor() as pool:
    for channel_grp in [ir10_red4_bg12_0, ir10_red4_bg12_1, ir11_red5_bg13_0, ir11_red5_bg13_1]:
        pool.submit(channel_grp.process)

"""
Then:
gdal_translate PSP_008831_1455.tif red.tif -b 2
gdal_translate PSP_008831_1455.tif green.tif -b 3
gdal_calc.py -A red.tif -B green.tif --calc='(B * 2) - (A * 0.3)' --outfile=blue.tif --overwrite
gdalbuildvrt -separate rgb.vrt red.tif green.tif blue.tif -overwrite
gdal_translate -colorinterp_1 red -colorinterp_2 green -colorinterp_3 blue rgb.vrt rgb.tif
"""