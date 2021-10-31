#!/usr/bin/env python3
from concurrent.futures import ThreadPoolExecutor
from sys import path as sys_path
from os.path import dirname, join as path_join, realpath
from logging import basicConfig as logConfig, getLogger, DEBUG, ERROR

# Thanks to
# https://repository.si.edu/bitstream/handle/10088/19366/nasm_201048.pdf?sequence=1&isAllowed=y
# https://github.com/USGS-Astrogeology/ISIS3/issues/3257#issuecomment-518856485

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

pdsimage2_paths = [
    "EDR/ESP/ORB_036600_036699/ESP_036618_1985/ESP_036618_1985",
    "EDR/ESP/ORB_037100_037199/ESP_037119_1985/ESP_037119_1985"
]

client = ISISClient("http://127.0.0.1:8080/api/v1")
processors = list()

threads = list()
with ThreadPoolExecutor() as pool:
    for image_path in pdsimage2_paths:
        detector_proc = HiRISEMultiDetectorProcessor(
            client,
            data_dir,
            image_path
        )
        t = pool.submit(detector_proc.process)
        threads.append(t)
        processors.append(detector_proc)

# Raise any errors thrown within the threads
[t.result() for t in threads]

final_mosaic = HiRISEMultiDetectorProcessor.mosaic(
    client,
    [p.mosaic for p in processors]
)
[client.delete(p.mosaic) for p in processors]


client.download(
    final_mosaic,
    path_join(output_dir, "jezero.cub")
)
client.delete(final_mosaic)
