from concurrent.futures import ThreadPoolExecutor
from sys import path as sys_path
from os.path import dirname, basename, join as path_join, exists as path_exists, realpath, splitext
from uuid import uuid4
from os import urandom
from urllib.parse import urlparse

pkg_dir = dirname(dirname(realpath(__file__)))
sys_path.insert(0, pkg_dir)

from isis_cloud.client import ISISClient


class HiRISEDetectorChannelProcessor:
    _PDS_IMAGE_PREFIX = "https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/HiRISE/{}/{}_{}_{}.IMG"

    def __init__(self, isis_client: ISISClient, download_dir: str, pdsimage_path: str, pdsimage_id: str, detector: str):
        self._isis_client = isis_client
        self._download_dir = download_dir
        self.image_id = pdsimage_id
        self.detector = detector
        self.channel0 = HiRISEDetectorChannelProcessor.get_img_url(
            pdsimage_path,
            pdsimage_id,
            detector,
            0
        )
        self.channel1 = HiRISEDetectorChannelProcessor.get_img_url(
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
        threads = list()
        with ThreadPoolExecutor() as pool:
            for channel in [self.channel0, self.channel1]:
                url_parsed = urlparse(channel)
                file_name = basename(url_parsed.path)
                out_file = path_join(self._download_dir, file_name)
                if not path_exists(out_file):
                    t = pool.submit(ISISClient.fetch, channel, out_file)
                    threads.append(t)
                channel_dls.append(out_file)

        [t.result() for t in threads]

        self.channel0 = channel_dls[0]
        self.channel1 = channel_dls[1]

    def _convert_to_cubes(self):
        cube_files = list()
        with ThreadPoolExecutor() as pool:
            for channel in [self.channel0, self.channel1]:
                t = pool.submit(
                    HiRISEDetectorChannelProcessor._convert_to_cube,
                    self._isis_client,
                    channel
                )
                cube_files.append(t)

        self._isis_client.delete(basename(self.channel0))
        self._isis_client.delete(basename(self.channel1))

        self.channel0 = cube_files[0].result()
        self.channel1 = cube_files[1].result()

    def _normalize_channels(self):
        normed_files = list()
        with ThreadPoolExecutor() as pool:
            for channel in [self.channel0, self.channel1]:
                t = pool.submit(
                    HiRISEDetectorChannelProcessor._normalize_channel,
                    self._isis_client,
                    channel
                )
                normed_files.append(t)

        self._isis_client.delete(self.channel0)
        self._isis_client.delete(self.channel1)

        self.channel0 = normed_files[0].result()
        self.channel1 = normed_files[1].result()

    def _stitch_channels(self):
        out_file = "{}_{}_{}.cub".format(
            self.image_id,
            self.detector,
            urandom(4).hex()
        )
        histitch = self._isis_client.program("histitch")
        histitch.add_arg("from1", self.channel0)
        histitch.add_arg("from2", self.channel1)
        histitch.add_arg("to", out_file)
        histitch.send()

        self._isis_client.delete(self.channel0)
        self._isis_client.delete(self.channel1)

        self.channel0 = None
        self.channel1 = None
        self.stitched = out_file

    @staticmethod
    def _convert_to_cube(client: ISISClient, hirise_img: str):
        cub_file = "{}.cub".format(uuid4())

        hi2isis = client.program("hi2isis")
        hi2isis.add_file_arg("from", hirise_img)
        hi2isis.add_arg("to", cub_file)
        hi2isis.send()

        spiceinit = client.program("spiceinit")
        spiceinit.add_arg("from", cub_file)
        spiceinit.add_arg("web", "true")
        spiceinit.send()

        cal_file = "{}.cub".format(uuid4())
        hical = client.program("hical")
        hical.add_arg("from", cub_file)
        hical.add_arg("to", cal_file)
        hical.send()

        client.delete(cub_file)

        return cal_file

    @staticmethod
    def _normalize_channel(isis_client: ISISClient, channel_file: str):
        norm_file = "{}.cub".format(uuid4())

        cubenorm = isis_client.program("cubenorm")
        cubenorm.add_arg("from", channel_file)
        cubenorm.add_arg("to", norm_file)
        cubenorm.send()

        return norm_file

    @staticmethod
    def get_img_url(pdsimage_path, pdsimage_id, detector, channel):
        return HiRISEDetectorChannelProcessor._PDS_IMAGE_PREFIX.format(
            pdsimage_path,
            pdsimage_id,
            detector,
            channel
        )
