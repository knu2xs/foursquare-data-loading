"""Make Foursquare Geoenrichment dataset."""

from configparser import ConfigParser
import datetime
import logging
from pathlib import Path
import importlib.util
import shutil
import subprocess
from typing import Optional, Union
from zipfile import ZipFile

import arcpy

for lib in ['arcpy_parquet', 'foursquare_data_loading']:
    if importlib.util.find_spec(lib) is None:
        raise EnvironmentError(
            f"The {lib} package is required. It was not found in the current environment."
        )

from arcpy_parquet import parquet_to_feature_class
from foursquare_data_loading import get_schema_csv


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


if __name__ == "__main__":

    # read and prep values from the config file
    config = ConfigParser()
    config_pth = Path(__file__).parent / "foursquare_conversion_config.ini"
    config.read(config_pth)

    input_dir = get_path_from_config("input_directory", "BASEMAPS")
    input_pqt = input_dir / 'parquet'
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

    # clean the target and download the data from S3
    raw_pth = Path(__file__).parent.parent / 'data' / 'raw' / 'foursquare_basemaps'
    if raw_pth.exists():
        logger.debug(f'Existing data download directory detected and starting to remove - {raw_pth}')
        shutil.rmtree(raw_pth)
        logger.debug('Removed data download directory.')

    logging.debug('Starting to download data from S3.')
    subprocess.run(f'aws s3 cp s3://esri-data-main/processed/delivery/foursquare_basemaps/ {str(raw_pth)} --recursive')
    logging.info('Data downloaded from S3.')

    # flush the geodatabase to avoid any corruption issues
    if arcpy.Exists(str(output_fgdb)):
        logging.debug(f'Removing previous file geodatabase - {output_fgdb}')
        arcpy.management.Delete(str(output_fgdb))
        logging.debug('Finished removing existing File Geodatabase.')

    # create the output file geodatabase
    logger.debug(f'Starting creation of File Geodatabase - {output_fgdb}')
    arcpy.management.CreateFileGDB(str(output_fgdb.parent), str(output_fgdb.stem))
    logger.info(f'File Geodatabase created - {output_fgdb}')

    # ensure the input parquet dataset exists
    if not input_pqt.exists():
        raise ValueError(f'Cannot locate the input parquet dataset {input_pqt}')

    # run the conversion
    logger.info('Starting parquet data import.')

    # get the path to the schema csv and ensure it exists
    schema_csv = get_schema_csv(input_dir / 'schema')

   # convert to feature class
    parquet_to_feature_class(
        parquet_path=input_pqt,
        output_feature_class=output_fc,
        schema_file=schema_csv,
        geometry_type='COORDINATES',
        geometry_column=['longitude', 'latitude'],
        spatial_reference=4326,
        build_spatial_index=True,
    )

    # location to save the archive
    zip_pth = output_dir / f'{output_fgdb.stem}.zip'

    logger.info(f'Starting to create an archive at {str(zip_pth)}')

    # build the archive
    with ZipFile(zip_pth, mode='w', compresslevel=9) as zipper:

        # iterate the files comprising the file geodatabase and add to the archive
        for gdb_file in output_dir.glob('**/*.gdb/**/*'):
            target_pth = str(gdb_file.relative_to(output_dir))
            zipper.write(gdb_file, target_pth)

    logger.info(f'Successfully created archive.')
