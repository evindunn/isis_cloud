#!/usr/bin/env python3
from concurrent.futures import ThreadPoolExecutor
from sys import path as sys_path
from os.path import dirname, join as path_join, realpath
from logging import basicConfig as logConfig, getLogger, DEBUG, ERROR


pkg_dir = dirname(dirname(realpath(__file__)))
sys_path.insert(0, pkg_dir)

from isis_cloud.client import ISISClient
from HiRISEMultiDetectorProcessor import HiRISEMultiDetectorProcessor


logConfig(
    level=DEBUG,
    datefmt="%F %T",
    format="[%(asctime)s][%(levelname)s][%(name)s] %(message)s"
)
getLogger("urllib3.connectionpool").setLevel(ERROR)

data_dir = "/data/disk/hirise_jezero"
output_dir = path_join(data_dir, "output")
pdsimage2_path = "EDR/ESP/ORB_036600_036699/ESP_036618_1985/ESP_036618_1985"

client = ISISClient("http://127.0.0.1:8080/api/v1")

multi_detector_processor = HiRISEMultiDetectorProcessor(
    client,
    data_dir,
    pdsimage2_path
)

multi_detector_processor.process()

product_id = pdsimage2_path.split("/")[-1]
client.download(
    multi_detector_processor.mosaic,
    path_join(output_dir, "{}.cub".format(product_id))
)

client.delete(multi_detector_processor.mosaic)
