#!/usr/bin/env python3
from concurrent.futures import ThreadPoolExecutor
from sys import path as sys_path
from os.path import dirname, basename, join as path_join, exists as path_exists, realpath, splitext
from os import makedirs, remove
from logging import basicConfig as logConfig, getLogger, DEBUG, ERROR
from tempfile import TemporaryDirectory
from urllib.parse import urlparse
from re import sub as re_sub
from os import urandom
from uuid import uuid4

# Thanks to
# https://repository.si.edu/bitstream/handle/10088/19366/nasm_201048.pdf?sequence=1&isAllowed=y
# https://github.com/USGS-Astrogeology/ISIS3/issues/3257#issuecomment-518856485
# https://www.lpi.usra.edu/meetings/lpsc2007/pdf/1779.pdf
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


def strip_ext(file_path):
    return file_path.split(".")[0]


class HiRISEDetectorProcessor:
    _PDS_IMAGE_PREFIX = "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/HiRISE/{}/{}_{}_{}.IMG"

    def __init__(self, isis_client: ISISClient, download_dir: str, pdsimage_path: str, pdsimage_id: str, detector: str):
        self._isis_client = isis_client
        self._download_dir = download_dir
        self.image_id = pdsimage_id
        self.detector = detector
        self.channel0 = HiRISEDetectorProcessor.get_img_url(
            pdsimage_path,
            pdsimage_id,
            detector,
            0
        )
        self.channel1 = HiRISEDetectorProcessor.get_img_url(
            pdsimage_path,
            pdsimage_id,
            detector,
            1
        )
        self.stitched = None

    def process(self):
        self._download_inputs()
        self._convert_to_cubes()
        self._normalize_channels()
        self._stitch_channels()

    def _download_inputs(self):
        channel_dls = list()
        with ThreadPoolExecutor() as pool:
            for channel in [self.channel0, self.channel1]:
                url_parsed = urlparse(channel)
                out_file = path_join(self._download_dir, basename(url_parsed.path))
                pool.submit(ISISClient.fetch, channel, out_file)
                channel_dls.append(out_file)

        self.channel0 = channel_dls[0]
        self.channel1 = channel_dls[1]

    def _convert_to_cubes(self):
        cube_files = list()
        with ThreadPoolExecutor() as pool:
            for channel in [self.channel0, self.channel1]:
                t = pool.submit(
                    HiRISEDetectorProcessor._convert_to_cube,
                    self._isis_client,
                    channel
                )
                cube_files.append(t)

        self._isis_client.rm(basename(self.channel0))
        self._isis_client.rm(basename(self.channel1))

        self.channel0 = cube_files[0].result()
        self.channel1 = cube_files[1].result()

    def _normalize_channels(self):
        normed_files = list()
        with ThreadPoolExecutor() as pool:
            for channel in [self.channel0, self.channel1]:
                t = pool.submit(
                    HiRISEDetectorProcessor._normalize_channel,
                    self._isis_client,
                    channel
                )
                normed_files.append(t)

        self._isis_client.rm(self.channel0)
        self._isis_client.rm(self.channel1)

        self.channel0 = normed_files[0].result()
        self.channel1 = normed_files[1].result()

    def _stitch_channels(self):
        out_file = "{}_{}.cub".format(self.image_id, self.detector)
        histitch = self._isis_client.command("histitch")
        histitch.add_arg("from1", self.channel0)
        histitch.add_arg("from2", self.channel1)
        histitch.add_arg("to", out_file)
        histitch.send()

        self.channel0 = None
        self.channel1 = None
        self.stitched = out_file

    @staticmethod
    def _convert_to_cube(client: ISISClient, hirise_img: str):
        hirise_basename = strip_ext(basename(hirise_img))

        cub_file = "{}.cub".format(hirise_basename)

        hi2isis = client.command("hi2isis")
        hi2isis.add_file_arg("from", hirise_img)
        hi2isis.add_arg("to", cub_file)
        hi2isis.send()

        spiceinit = client.command("spiceinit")
        spiceinit.add_arg("from", cub_file)
        spiceinit.add_arg("web", "true")
        spiceinit.send()

        cal_file = "{}.cal.cub".format(hirise_basename)
        hical = client.command("hical")
        hical.add_arg("from", cub_file)
        hical.add_arg("to", cal_file)
        hical.send()

        client.rm(cub_file)

        return cal_file

    @staticmethod
    def _normalize_channel(isis_client: ISISClient, channel_file: str):
        norm_file = "{}.norm.cub".format(strip_ext(channel_file))

        cubenorm = isis_client.command("cubenorm")
        cubenorm.add_arg("from", channel_file)
        cubenorm.add_arg("to", norm_file)
        cubenorm.send()

        return norm_file

    @staticmethod
    def get_img_url(pdsimage_path, pdsimage_id, detector, channel):
        return HiRISEDetectorProcessor._PDS_IMAGE_PREFIX.format(
            pdsimage_path,
            pdsimage_id,
            detector,
            channel
        )


class HiRISETrueColorProcessor:
    def __init__(self, isis_client: ISISClient, image_id: str, red_cube: str, blue_green_cube: str):
        self._isis_client = isis_client
        self._img_id = image_id
        self.red = red_cube
        self.bg = blue_green_cube
        self.synth_blue = None
        self.combined = None
        self._red_orig_binning = 0
        self._bg_orig_binning = 0
        self._red_orig_size = [0, 0]

    def process(self):
        self._size_match_bg()
        self._fix_jitter()
        self._propagate_red_highfreq()
        self._generate_synth_blue()
        self._combine()
        self._reproject()

    def _size_match_bg(self):
        with TemporaryDirectory() as tmp_dir:
            red_dl = path_join(tmp_dir, self.red)
            bg_dl = path_join(tmp_dir, self.bg)

            with ThreadPoolExecutor() as pool:
                for file, dl_file in [(self.red, red_dl), (self.bg, bg_dl)]:
                    pool.submit(
                        self._isis_client.download,
                        file,
                        dl_file
                    )

            red_lbl = ISISClient.parse_cube_label(red_dl)
            bg_lbl = ISISClient.parse_cube_label(bg_dl)

            self._red_orig_binning = red_lbl["IsisCube"]["Instrument"]["Summing"]
            self._bg_orig_binning = bg_lbl["IsisCube"]["Instrument"]["Summing"]

            self._red_orig_size[0] = int(red_lbl["IsisCube"]["Core"]["Dimensions"]["Samples"])
            self._red_orig_size[1] = int(red_lbl["IsisCube"]["Core"]["Dimensions"]["Lines"])

            optical_distortion_correct = 1.0006

            scale_factor = (
                float(self._bg_orig_binning) /
                float(self._red_orig_binning) *
                optical_distortion_correct
            )

            enlarged_file = "{}.enlarged.cub".format(strip_ext(self.bg))

            enlarge = self._isis_client.command("enlarge")
            enlarge.add_arg("from", self.bg)
            enlarge.add_arg("interp", "bilinear")
            enlarge.add_arg("sscale", scale_factor)
            enlarge.add_arg("lscale", scale_factor)
            enlarge.add_arg("to", enlarged_file)
            enlarge.send()

            cropped_file = "{}.cropped.cub".format(strip_ext(self.bg))

            crop = self._isis_client.command("crop")
            crop.add_arg("from", enlarged_file)
            crop.add_arg("nsamples", self._red_orig_size[0])
            crop.add_arg("nlines", self._red_orig_size[1])
            crop.add_arg("to", cropped_file)
            crop.send()

            self._isis_client.rm(enlarged_file)

            editlab = self._isis_client.command("editlab")
            editlab.add_arg("from", cropped_file)
            editlab.add_arg("grpname", "Instrument")
            editlab.add_arg("keyword", "Summing")
            editlab.add_arg("value", self._red_orig_binning)
            editlab.send()

            self._isis_client.rm(self.bg)
            self.bg = cropped_file

    def _fix_jitter(self):
        bg_basename = strip_ext(self.bg)
        autoreg_file = "{}.autoreg".format(uuid4())
        cnet_file = "{}.cnet".format(uuid4())
        slithered_file = "{}.slither.cub".format(bg_basename)

        # Match the first 1/32 of the bg image to some area within the first
        # 1/16 of the red image with at least 90% correlation. It takes a while
        # but I couldn't get red/bg lined up using the default autoreg template
        autoreg = self._isis_client.command("autoregtemplate")
        autoreg.add_arg("algorithm", "MaximumCorrelation")
        autoreg.add_arg("tolerance", 0.9)
        autoreg.add_arg("psamp", self._red_orig_size[0] // 32)
        autoreg.add_arg("pline", self._red_orig_size[1] // 32)
        autoreg.add_arg("ssamp", self._red_orig_size[0] // 16)
        autoreg.add_arg("sline", self._red_orig_size[1] // 16)
        autoreg.add_arg("topvl", autoreg_file)
        autoreg.send()

        hijitreg = self._isis_client.command("hijitreg")
        hijitreg.add_arg("from", self.bg)
        hijitreg.add_arg("match", self.red)
        hijitreg.add_arg("regdef", autoreg_file)
        hijitreg.add_arg("cnetfile", cnet_file)
        hijitreg.send()

        slither = self._isis_client.command("slither")
        slither.add_arg("from", self.bg)
        slither.add_arg("control", cnet_file)
        slither.add_arg("to", slithered_file)
        slither.send()

        self._isis_client.rm(cnet_file)
        self._isis_client.rm(autoreg_file)
        self._isis_client.rm(self.bg)

        self.bg = slithered_file

    def _propagate_red_highfreq(self):
        bg_basename = strip_ext(self.bg)

        ratio_img = "{}.ratio.cub".format(bg_basename)
        ratio_img_lowpass = "{}.lowpass.cub".format(bg_basename)
        bg_img_filtered = "{}.filtered.cub".format(bg_basename)

        ratio = self._isis_client.command("ratio")
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

        lowpass = self._isis_client.command("lowpass")
        lowpass.add_arg("from", ratio_img)
        lowpass.add_arg("samples", boxcar_size)
        lowpass.add_arg("lines", boxcar_size)
        lowpass.add_arg("to", ratio_img_lowpass)
        lowpass.send()

        self._isis_client.rm(ratio_img)

        fx = self._isis_client.command("fx")
        fx.add_arg("f1", ratio_img_lowpass)
        fx.add_arg("f2", self.red)
        fx.add_arg("equation", "f1 * f2")
        fx.add_arg("to", bg_img_filtered)
        fx.send()

        self._isis_client.rm(ratio_img_lowpass)
        self._isis_client.rm(self.bg)

        self.bg = bg_img_filtered

    def _generate_synth_blue(self):
        self.synth_blue = "{}_{}.cub".format(
            re_sub(r"RED\d", "BLUE", strip_ext(self.red)),
            urandom(4).hex()
        )

        fx = self._isis_client.command("fx")
        fx.add_arg("f1", self.red)
        fx.add_arg("f2", self.bg)
        fx.add_arg("equation", "[2 * f2] - [0.3 * f1]")
        fx.add_arg("to", self.synth_blue)
        fx.send()

    def _combine(self):
        self.combined = "{}.cub".format(image_id)
        cubeit = self._isis_client.command("cubeit")
        cubeit.add_arg("fromlist", [self.red, self.bg, self.synth_blue])
        cubeit.add_arg("to", self.combined)
        cubeit.send()

    def _reproject(self):
        map_file = "{}.map".format(uuid4())
        maptemplate = self._isis_client.command("maptemplate")
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

        mapped_file = "{}.combined.cub".format(strip_ext(self.combined))

        cam2map = self._isis_client.command("cam2map")
        cam2map.add_arg("from", self.combined)
        cam2map.add_arg("map", map_file)
        cam2map.add_arg("pixres", "mpp")
        cam2map.add_arg("resolution", resolution)
        cam2map.add_arg("to", mapped_file)
        cam2map.send()

        self._isis_client.rm(self.combined)
        self.combined = mapped_file

        self._isis_client.rm(map_file)
        self._isis_client.rm(self.red)
        self._isis_client.rm(self.bg)
        self._isis_client.rm(self.synth_blue)

        self.red = None
        self.bg = None
        self.synth_blue = None


class HiRISECCDProcessor:
    pass


data_dir = "/data/disk/hirise_jezero"
output_dir = path_join(data_dir, "output")
client = ISISClient("http://127.0.0.1:8080/api/v1")

for directory in [data_dir, output_dir]:
    if not path_exists(directory):
        makedirs(directory)

image_path = "EDR/ESP/ORB_036600_036699/ESP_036618_1985"
image_id = "ESP_036618_1985"
detectors = ["RED4", "BG12"]
detector_processors = list()

with ThreadPoolExecutor() as pool:
    for detector in detectors:
        detector_proc = HiRISEDetectorProcessor(
            client,
            data_dir,
            image_path,
            image_id,
            detector
        )
        pool.submit(detector_proc.process)
        detector_processors.append(detector_proc)

for detector in detector_processors:
    client.download(detector.stitched, path_join(output_dir, detector.stitched))

color_proc = HiRISETrueColorProcessor(
    client,
    image_id,
    detector_processors[0].stitched,
    detector_processors[1].stitched
)
color_proc.process()

client.download(color_proc.combined, path_join(output_dir, color_proc.combined))
client.rm(color_proc.combined)
