"""Make Foursquare Geoenrichment dataset."""

from configparser import ConfigParser
import datetime
import importlib.util
import logging
from pathlib import Path
import shutil
import subprocess
from typing import Optional, Union
from zipfile import ZipFile

import arcpy

if importlib.util.find_spec("arcpy_parquet") is None:
    raise EnvironmentError(
        "The arcpy_parquet package is required. It was not found in the current environment."
    )

from arcpy_parquet import parquet_to_feature_class

if importlib.util.find_spec("py_message") is not None:
    from py_message import send_pushover
    has_py_message = True
else:
    has_py_message = False


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

    delivery_year = config.get('DEFAULT', 'delivery_year')
    delivery_month = config.get('DEFAULT', 'delivery_month')
    s3_pth = config.get('DATA_PACKS', 's3_source')
    pqt_pth = config.get("DATA_PACKS", "input_directory")
    output_dir = config.get("DATA_PACKS", "output_directory")

    # create path to where achives will be stored
    zip_dir = output_dir.parent / f'{output_dir.stem}_archive'

    # create parquet path to specific month - ensures delivery_* columns are not included in built data
    pqt_date_pth = pqt_pth / f'delivery_year={delivery_year}' / f'delivery_month={delivery_month}'

    if not pqt_date_pth.exists():
        raise FileNotFoundError(f"""The dataset for the specified year and month does not appear to exist, "{pqt_date_pth}\"""")
    else:
        logging.info(f"""Using parquet dataset located at "{pqt_date_pth}\"""")

    # create path to output data directory
    out_dir = output_dir / f'delivery_year={delivery_year}' / f'delivery_month={delivery_month}'

    # ensure the output targets exist for the output
    if not out_dir.exists():
        out_dir.mkdir(parents=True)

    # set up logging
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # configure the logging formattter
    log_fmt = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')

    # configure and add the logging file handler
    timestamp_str = datetime.datetime.today().strftime('%Y%m%d')
    fh = logging.FileHandler(str(output_dir / f"foursquare_data_packs_{timestamp_str}.log"))
    fh.setFormatter(log_fmt)
    logger.addHandler(fh)

    # ensure logging messages still go to the console
    sh = logging.StreamHandler()
    sh.setFormatter(log_fmt)
    logger.addHandler(sh)

    # clean the target and download the data from S3
    raw_pth = Path(__file__).parent.parent / 'data' / 'raw' / input_dir.stem
    if raw_pth.exists():
        logger.debug(f'Existing data download directory detected and starting to remove - {raw_pth}')
        shutil.rmtree(raw_pth)
        logger.debug('Removed data download directory.')

    logging.debug('Starting to download data from S3.')
    cmd_str = f'aws s3 cp {str(s3_pth)}/ {str(raw_pth)} --recursive'
    subprocess.run(cmd_str)
    logging.info('Data downloaded from S3.')

    # get the path to the schema csv and ensure it exists
    schema_csv = get_schema_csv(pqt_pth.parent / 'schema')

    # get unique country paths
    pth_set = set(pth.parent for pth in pqt_pth.rglob('*.parquet'))

    # iterate the unique countries
    for pth in pth_set:

        # get the part of the path defining the country
        cntry_prt = [prt for prt in pth.parts if prt.startswith('country')][0]
        
        # location to save the exported country data
        cntry_dir = out_dir / cntry_prt
        
        # make sure the directory exists
        if not cntry_dir.exists():
            cntry_dir.mkdir(parents=True)
        
        # create path to feature class
        fc_pth = cntry_dir / 'foursquare.gdb' / 'places'
        
        # create the file geodatabase to hydrate
        if arcpy.Exists(str(fc_pth.parent)):
            arcpy.management.Delete(str(fc_pth.parent))
            
        with arcpy.EnvManager(overwriteOutput=True):
            _ = arcpy.management.CreateFileGDB(str(cntry_dir), 'foursquare.gdb')
        
        # convert the data to a feature class
        parquet_to_feature_class(
            parquet_path=pqt_date_pth, 
            output_feature_class=fc_pth, 
            schema_file=schema_csv, 
            parquet_partitions=[cntry_prt], 
            geometry_type='COORDINATES',
            geometry_column=('longitude', 'latitude'),
            build_spatial_index=True, compact=True
        )
        
        logging.info(f'Successfully created {fc_pth}')

        # get the path to the file geodatabase from the feautre class path
        fgdb_pth = fc_pth.parent

        # create a path to save the zipped archive
        zip_pth = zip_dir / f'{fgdb_pth.parent.stem}.zip'

        # ensure the location to save the archive exists
        if not zip_pth.parent.exists():
            zip_pth.parent.mkdir(parents=True)
        
        logging.info(f'Starting to create an archive at {str(zip_pth)}')

        # build the archive
        with ZipFile(zip_pth, mode='w', compresslevel=9) as zipper:
        
            # iterate the files in the file geodatabase
            for gdb_file in fgdb_pth.rglob('*'):

                # ignore lock files...they create problems
                if not gdb_file.suffix == '.lock':
        
                    # create a path in the archive with the file geodatabase
                    target_pth = gdb_file.relative_to(fgdb_pth.parent)
            
                    # add the file to the archive
                    zipper.write(gdb_file, target_pth)
        
        logging.info(f'Successfully created archive.')

    if has_py_message:
        send_pushover('Finished building Foursquare data packs.')