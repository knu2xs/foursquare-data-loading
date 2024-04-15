"""Make Foursquare basemaps dataset."""

from configparser import ConfigParser
import datetime
import logging
from pathlib import Path
import pkgutil
from typing import Optional, Union
from zipfile import ZipFile

import arcpy

if pkgutil.get_loader("arcpy_parquet") is None:
    raise EnvironmentError(
        "The arcpy_parquet package is required. It was not found in the current environment."
    )

from arcpy_parquet import parquet_to_feature_class


def get_path_from_config(key: str, section: Optional[str] = 'DEFAULT') -> Path:
    """
    Helper function to retrieve relative paths from the config, and return absolute paths.
    """
    # create a path to start with by retrieving the value from the config file
    pth = Path(config.get(section, key).strip('"'))

    # get the directory this file is located in as a point of reference
    top_dir = Path(__file__).parent.parent

    # iterate through the parts, and correctly prepend the data directory with the path
    for prt in pth.parts:
        if prt.endswith('..'):
            top_dir = top_dir.parent
            pth = Path(*pth.parts[1:])

        elif prt == 'data':
            pth = top_dir / pth

        # if not necessary to go up a directory level, get the full path and be done
        else:
            pth = pth.absolute()
            break

    return pth


def get_schema_csv(schema_dir: Union[Path, str]) -> Path:
    """Helper function to retrieve the csv for the schema when saved as a single part file from Spark."""
    # ensure we are working with a Path object
    if isinstance(schema_dir, str):
        schema_dir = Path(schema_dir)

    # if working in a directory
    if schema_dir.is_dir():

        # get part csv file
        prt_lst = [fl for fl in schema_dir.glob('part-*.csv')]

        # ensure there even is a part file to work with
        if len(prt_lst) == 0:
            raise ValueError('Cannot locate a part*.csv file in the directory tree.')
        else:
            schema_csv = prt_lst[0]

    # if just the file was passed
    elif schema_dir.suffix == '.csv':
        schema_csv = schema_dir

    # pitch a fit if cannot figure out what to  do
    else:
        raise ValueError('Cannot locate a schema *.csv file.')
    
    return schema_csv


if __name__ == "__main__":

    # read and prep values from the config file
    config = ConfigParser()
    config_pth = Path(__file__).parent / "foursquare_conversion_config.ini"
    config.read(config_pth)

    input_dir = get_path_from_config("input_directory", "BASEMAPS")
    input_pqt = input_dir / 'parquet'
    schema_csv = get_schema_csv(input_dir / 'schema')
    output_fc = get_path_from_config("output_feature_class", "BASEMAPS")

    output_fgdb = output_fc.parent
    output_dir = output_fgdb.parent

    # ensure the output targets exist for the output
    if not output_dir.exists():
        output_dir.mkdir(parents=True)

    # set up logging
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # configure the logging formattter
    log_fmt = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')

    # configure and add the logging file handler
    timestamp_str = datetime.datetime.today().strftime('%Y%m%d')
    fh = logging.FileHandler(str(output_dir / f"foursquare_basemaps_{timestamp_str}.log"))
    fh.setFormatter(log_fmt)
    logger.addHandler(fh)

    # ensure logging messages still go to the console
    sh = logging.StreamHandler()
    sh.setFormatter(log_fmt)
    logger.addHandler(sh)

    # flush the geodatabase to avoid any corruption issues
    if arcpy.Exists(str(output_fgdb)):
        arcpy.management.Delete(str(output_fgdb))

    # create the output file geodatabase
    logging.info(f'Starting creation of File Geodatabase - {output_fgdb}')
    arcpy.management.CreateFileGDB(str(output_fgdb.parent), str(output_fgdb.stem))
    logging.info(f'Finished creation of File Geodatabase.')

    # ensure the input parquet dataset exists
    if not input_pqt.exists():
        raise ValueError(f'Cannot locate the input parquet dataset {input_pqt}')

    # run the conversion
    logging.info('Starting parquet data import.')

    # convert to feature class
    parquet_to_feature_class(
        parquet_path=input_pqt,
        output_feature_class=output_fc,
        schema_file=schema_csv,
        spatial_reference=3857,
        build_spatial_index=True
    )

    # location to save the archive
    zip_pth = output_dir / f'{output_fgdb.stem}.zip'

    logging.info(f'Starting to create an archive at {str(zip_pth)}')

    # compress the file geodatabase
    with ZipFile(zip_pth, mode='w', compresslevel=9) as zipper:

        # iterate the files comprising the file geodatabase and add to the archive
        for gdb_file in output_dir.glob('**/*.gdb/**/*'):
            target_pth = str(gdb_file.relative_to(output_dir))
            zipper.write(gdb_file, target_pth)

    logging.info(f'Successfully created archive.')
