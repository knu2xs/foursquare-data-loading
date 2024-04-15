SETLOCAL
SET PROJECT_DIR="%~dp0..\"

del /f/q/s ..\data\raw\foursquare_basemaps\*.* > null
rmdir /q/s ..\data\raw\foursquare_basemaps\

aws s3 cp s3://esri-data-main/processed/delivery/foursquare_basemaps/ ..\data\raw\foursquare_basemaps\ --recursive
