#!/usr/bin/env python3
import concurrent.futures
import logging
from concurrent.futures import ThreadPoolExecutor
from sys import path as sys_path
from os.path import dirname, basename, join as path_join, exists as path_exists, realpath, splitext
from os import makedirs, remove
from logging import basicConfig as logConfig, getLogger, DEBUG, ERROR
from tempfile import TemporaryDirectory
from urllib.parse import urlparse
from re import sub as re_sub
from os import urandom

# Thanks to
# https://repository.si.edu/bitstream/handle/10088/19366/nasm_201048.pdf?sequence=1&isAllowed=y
# https://github.com/USGS-Astrogeology/ISIS3/issues/3257#issuecomment-518856485
# for this
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


data_dir = "/data/disk/hirise_jezero"
output_dir = path_join(data_dir, "output")
client = ISISClient("http://127.0.0.1:8080/api/v1")


class HiRISEColorSetProcessor:
    def __init__(self, combined_file, red_url, bg_url):
        dl_files = list()

        with ThreadPoolExecutor() as pool:
            for url in [red_url, bg_url]:
                url_parsed = urlparse(url)
                dl_file = path_join(data_dir, basename(url_parsed.path))
                dl_files.append(dl_file)
                if not path_exists(dl_file):
                    pool.submit(ISISClient.fetch, url, dl_file)

        self.red = dl_files[0]
        self.bg = dl_files[1]
        self.synth_blue = "{}_{}.cub".format(
            re_sub(r"RED\d", "BLUE", HiRISEColorSetProcessor.strip_ext(basename(self.red))),
            urandom(4).hex()
        )
        self.combined = combined_file

        self._red_orig_binning = 0
        self._bg_orig_binning = 0

    def process(self):
        self._convert_to_cubes()
        self._enlarge_bg()
        self._fix_jitter()
        self._generate_synth_blue()
        self._propagate_red_highfreq()
        self._combine()
        self._reproject()

    def _convert_to_cubes(self):
        threads = list()
        with ThreadPoolExecutor() as pool:
            for file in [self.red, self.bg]:
                threads.append(
                    pool.submit(HiRISEColorSetProcessor._convert_to_cube, file)
                )

        self.red = threads[0].result()
        self.bg = threads[1].result()

    def _enlarge_bg(self):
        with TemporaryDirectory() as tmp_dir:
            red_dl = path_join(tmp_dir, self.red)
            bg_dl = path_join(tmp_dir, self.bg)

            with ThreadPoolExecutor() as pool:
                for file, dl_file in [(self.red, red_dl), (self.bg, bg_dl)]:
                    pool.submit(
                        client.download,
                        file,
                        dl_file
                    )

            red_lbl = ISISClient.parse_cube_label(red_dl)
            bg_lbl = ISISClient.parse_cube_label(bg_dl)

            optical_distortion_correct = 1.0006

            self._red_orig_binning = red_lbl["IsisCube"]["Instrument"]["Summing"]
            self._bg_orig_binning = bg_lbl["IsisCube"]["Instrument"]["Summing"]

            red_samples = red_lbl["IsisCube"]["Core"]["Dimensions"]["Samples"]
            red_lines = red_lbl["IsisCube"]["Core"]["Dimensions"]["Lines"]

            scale_factor = (
                float(self._bg_orig_binning) /
                float(self._red_orig_binning) *
                optical_distortion_correct
            )

            enlarged_file = "{}.enlarged.cub".format(
                HiRISEColorSetProcessor.strip_ext(self.bg)
            )

            enlarge = client.command("enlarge")
            enlarge.add_arg("from", self.bg)
            enlarge.add_arg("interp", "bilinear")
            enlarge.add_arg("sscale", scale_factor)
            enlarge.add_arg("lscale", scale_factor)
            enlarge.add_arg("to", enlarged_file)
            enlarge.send()

            cropped_file = "{}.cropped.cub".format(
                HiRISEColorSetProcessor.strip_ext(self.bg)
            )

            crop = client.command("crop")
            crop.add_arg("from", enlarged_file)
            crop.add_arg("nsamples", red_samples)
            crop.add_arg("nlines", red_lines)
            crop.add_arg("to", cropped_file)
            crop.send()

            client.rm(enlarged_file)

            editlab = client.command("editlab")
            editlab.add_arg("from", cropped_file)
            editlab.add_arg("grpname", "Instrument")
            editlab.add_arg("keyword", "Summing")
            editlab.add_arg("value", self._red_orig_binning)
            editlab.send()

            client.rm(self.bg)
            self.bg = cropped_file

    def _fix_jitter(self):
        bg_basename = HiRISEColorSetProcessor.strip_ext(self.bg)
        cnet_file = "{}.cnet".format(bg_basename)
        slithered_file = "{}.slither.cub".format(bg_basename)

        hijitreg = client.command("hijitreg")
        hijitreg.add_arg("from", self.bg)
        hijitreg.add_arg("match", self.red)
        hijitreg.add_arg("cnetfile", cnet_file)
        hijitreg.send()

        slither = client.command("slither")
        slither.add_arg("from", self.bg)
        slither.add_arg("control", cnet_file)
        slither.add_arg("to", slithered_file)
        slither.send()

        client.rm(cnet_file)
        client.rm(self.bg)

        self.bg = slithered_file

    def _generate_synth_blue(self):
        fx = client.command("fx")
        fx.add_arg("f1", self.bg)
        fx.add_arg("f2", self.red)
        fx.add_arg("equation", "[2 * f1] - [0.3 * f2]")
        fx.add_arg("to", self.synth_blue)
        fx.send()

    def _propagate_red_highfreq(self):
        bg_basename = HiRISEColorSetProcessor.strip_ext(self.bg)

        ratio_img = "{}_red.cub".format(bg_basename)
        ratio_img_lowpass = "{}_red.lowpass.cub".format(bg_basename)
        bg_img_filtered = "{}.filtered.cub".format(bg_basename)

        ratio = client.command("ratio")
        ratio.add_arg("numerator", self.bg)
        ratio.add_arg("denominator", self.red)
        ratio.add_arg("to", ratio_img)
        ratio.send()

        if self._bg_orig_binning == 2:
            boxcar_size = 3
        elif self._bg_orig_binning == 4:
            boxcar_size = 5
        else:
            err = "Invalid binning for {} (got {}, expected 2 or 4)".format(
                self.bg,
                self._bg_orig_binning
            )
            raise RuntimeError(err)

        lowpass = client.command("lowpass")
        lowpass.add_arg("from", ratio_img)
        lowpass.add_arg("samples", boxcar_size)
        lowpass.add_arg("lines", boxcar_size)
        lowpass.add_arg("to", ratio_img_lowpass)
        lowpass.send()

        client.rm(ratio_img)

        fx = client.command("fx")
        fx.add_arg("f1", ratio_img_lowpass)
        fx.add_arg("f2", self.red)
        fx.add_arg("equation", "f1 * f2")
        fx.add_arg("to", bg_img_filtered)
        fx.send()

        client.rm(ratio_img_lowpass)
        client.rm(self.bg)

        self.bg = bg_img_filtered

    def _combine(self):
        cubeit = client.command("cubeit")
        cubeit.add_arg("fromlist", [self.red, self.bg, self.synth_blue])
        cubeit.add_arg("to", self.combined)
        cubeit.send()

    def _reproject(self):
        map_file = "{}.map".format(uuid4())
        maptemplate = client.command("maptemplate")
        maptemplate.add_arg("projection", "equirectangular")
        maptemplate.add_arg("clat", 0.0)
        maptemplate.add_arg("clon", 0.0)
        maptemplate.add_arg("map", map_file)
        maptemplate.send()

        if self._red_orig_binning == 1:
            resolution = 0.25
        elif self._red_orig_binning == 2:
            resolution = 0.5
        else:
            resolution = 1.0

        mapped_file = "{}.combined.cub".format(
            HiRISEColorSetProcessor.strip_ext(self.combined)
        )

        cam2map = client.command("cam2map")
        cam2map.add_arg("from", self.combined)
        cam2map.add_arg("map", map_file)
        cam2map.add_arg("pixres", "mpp")
        cam2map.add_arg("resolution", resolution)
        cam2map.add_arg("to", mapped_file)
        cam2map.send()

        client.rm(self.combined)
        client.rm(map_file)
        self.combined = mapped_file

    def cleanup(self):
        for file in [self.red, self.bg, self.synth_blue, self.combined]:
            client.rm(file)

    @staticmethod
    def strip_ext(file_name):
        return basename(file_name).split(".")[0]

    @staticmethod
    def _convert_to_cube(hirise_img):
        hirise_basename = HiRISEColorSetProcessor.strip_ext(hirise_img)

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
    def _reproject_single(cube_file, map_file, orig_binning):
        cube_file_mapped = "{}.mapped.cub".format(
            HiRISEColorSetProcessor.strip_ext(cube_file)
        )

        if orig_binning == 1:
            resolution = 0.25
        elif orig_binning == 2:
            resolution = 0.5
        elif orig_binning == 4:
            resolution = 1.0
        else:
            err = "Invalid binning for {} (expected 1, 2, or 4, got {})".format(
                cube_file,
                orig_binning
            )
            raise RuntimeError(err)

        cam2map = client.command("cam2map")
        cam2map.add_arg("from", cube_file)
        cam2map.add_arg("map", map_file)
        cam2map.add_arg("to", cube_file_mapped)
        cam2map.add_arg("pixres", "mpp")
        cam2map.add_arg("resolution", resolution)
        cam2map.send()

        client.rm(cube_file)

        return cube_file_mapped


for directory in [data_dir, output_dir]:
    if not path_exists(directory):
        makedirs(directory)

image_path = "EDR/PSP/ORB_008800_008899/PSP_008831_1455"
image_id = "PSP_008831_1455"

red4_bg12_0 = HiRISEColorSetProcessor(
    "{}_RED4_BG12.cub".format(image_id),
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/HiRISE/{}/{}_RED4_0.IMG".format(image_path, image_id),
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/HiRISE/{}/{}_BG12_0.IMG".format(image_path, image_id),
)

red5_bg13_0 = HiRISEColorSetProcessor(
    "{}_RED5_BG13.cub".format(image_id),
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/HiRISE/{}/{}_RED5_0.IMG".format(image_path, image_id),
    "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/HiRISE/{}/{}_BG13_0.IMG".format(image_path, image_id),
)

# ThreadPool inception
with ThreadPoolExecutor() as pool:
    for color_set in [red4_bg12_0, red5_bg13_0]:
        pool.submit(color_set.process)

red_bg_cube = "{}.cub".format(image_id)
automos = client.command("automos")
automos.add_arg("fromlist", [red4_bg12_0.combined, red5_bg13_0.combined])
automos.add_arg("mosaic", red_bg_cube)
automos.send()

red4_bg12_0.cleanup()
red5_bg13_0.cleanup()

blue_cub = "{}.blue.cub".format(image_id)
fx = client.command("fx")
fx.add_arg("f1", "{}+1".format(red_bg_cube))
fx.add_arg("f2", "{}+2".format(red_bg_cube))
fx.add_arg("equation", "[(f2 * 2) - (f1 * 0.3)]")
fx.add_arg("to", blue_cub)
fx.send()

out_tif = "{}.tif".format(image_id)
isis2std = client.command("isis2std")
isis2std.add_arg("mode", "rgb")
isis2std.add_arg("format", "tiff")
isis2std.add_arg("red", "{}+1".format(red_bg_cube))
isis2std.add_arg("green", "{}+2".format(red_bg_cube))
isis2std.add_arg("blue", blue_cub)
isis2std.add_arg("to", out_tif)
isis2std.send()

client.rm(red_bg_cube)
client.rm(blue_cub)

client.download(out_tif, path_join(output_dir, out_tif))

client.rm(out_tif)
