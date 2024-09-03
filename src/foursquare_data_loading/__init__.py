__title__ = "foursquare-data-loading"
__version__ = "0.0.0"
__author__ = "Joel McCune (https://github.com/knu2xs)"
__license__ = "Apache 2.0"
__copyright__ = "Copyright 2023 by Joel McCune (https://github.com/knu2xs)"

__all__ = ["get_schema_csv", "utils"]

from typing import Union
from pathlib import Path

import pandas as pd

# add specific imports below if you want to organize your code into modules, which is mostly what I do
from . import utils


def get_schema_csv(schema_dir: Union[Path, str]) -> Path:
    """Helper function to retrieve the csv for the schema when saved as a single part file from Spark."""
    # ensure we are working with a Path object
    if isinstance(schema_dir, str):
        schema_dir = Path(schema_dir)

    # if working in a directory
    if schema_dir.is_dir():
        # get part csv file
        prt_lst = [fl for fl in schema_dir.glob("part-*.csv")]

        # ensure there even is a part file to work with
        if len(prt_lst) == 0:
            raise ValueError("Cannot locate a part*.csv file in the directory tree.")
        else:
            schema_csv = prt_lst[0]

    # if just the file was passed
    elif schema_dir.suffix == ".csv":
        schema_csv = schema_dir

    # pitch a fit if cannot figure out what to  do
    else:
        raise ValueError("Cannot locate a schema *.csv file.")

    return schema_csv


