SETLOCAL
SET PROJECT_DIR="%~dp0..\"

aws s3 cp s3://esri-data-main/processed/delivery/foursquare_geoenrichment/ %PROJECT_DIR%data\raw\
aws s3 cp s3://esri-data-main/processed/delivery/foursquare_basemaps/ %PROJECT_DIR%data\raw\