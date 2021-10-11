from os import environ
from os.path import join as path_join


if "ISISROOT" not in environ.keys():
    environ["ISISROOT"] = environ["CONDA_PREFIX"]

if "ISISDATA" not in environ.keys():
    environ["ISISDATA"] = path_join(environ["ISISROOT"], "data")

