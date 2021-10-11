from os import getenv, getcwd
from os.path import join as path_join


class ISISServerConfig:
    _WORK_DIR = getenv("DATA_DIR", path_join(getcwd(), ".work"))

    @staticmethod
    def work_dir():
        return ISISServerConfig._WORK_DIR
