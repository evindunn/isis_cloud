from logging import getLogger

import connexion
from threading import Thread
from time import sleep, time
from os import listdir, stat as file_stat, remove
from os.path import join as path_join, basename
from ._config import ISISServerConfig


class ISISServer(connexion.FlaskApp):
    _CLEANUP_LOGGER = getLogger("FileCleanup")
    _DELETE_FILES_AFTER = 3600 * 24

    def __init__(self):
        super().__init__(
            __name__,
            specification_dir="openapi",
            options={"swagger_url": "/docs"}
        )
        self.add_api("main.yml")
        cleanup_thread = Thread(target=ISISServer._file_cleanup, daemon=True)
        cleanup_thread.start()

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
                    "Cleaning stale job file {}".format(basename(file))
                )
                removed_files = True
                remove(file)

        if not removed_files:
            ISISServer._CLEANUP_LOGGER.info("No stale job file to remove")

        sleep(ISISServer._DELETE_FILES_AFTER)
