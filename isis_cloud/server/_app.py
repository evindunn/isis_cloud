from logging import getLogger

import connexion
from time import sleep, time
from os import listdir, stat as file_stat, remove, removedirs
from os.path import join as path_join, basename, isdir
from ._config import ISISServerConfig


class ISISServer(connexion.FlaskApp):
    _CLEANUP_LOGGER = getLogger("FileCleanup")
    # Remove stale files once per day
    _DELETE_FILES_AFTER = 3600 * 24

    def __init__(self):
        super().__init__(
            __name__,
            specification_dir="openapi",
            options={"swagger_url": "/docs"}
        )
        self.add_api("main.yml")

    @staticmethod
    def _file_cleanup():
        now = time()
        job_files = listdir(ISISServerConfig.work_dir())
        removed_files = False

        for file in job_files:
            file = path_join(ISISServerConfig.work_dir(), file)
            stats = file_stat(file)
            if now - stats.st_mtime > ISISServer._DELETE_FILES_AFTER:
                ISISServer._CLEANUP_LOGGER.info(
                    "Cleaning stale job file/directory {}".format(basename(file))
                )
                removed_files = True
                if isdir(file):
                    removedirs(file)
                else:
                    remove(file)

        if not removed_files:
            ISISServer._CLEANUP_LOGGER.info("No stale job files to remove")

        sleep(ISISServer._DELETE_FILES_AFTER)
