SETLOCAL
SET PROJECT_DIR=%~dp0..\

del /f/q/s ..\data\raw\foursquare_geoenrichment\*.* > null
rmdir /q/s ..\data\raw\foursquare_geoenrichment\

aws s3 cp s3://esri-data-main/processed/delivery/foursquare_geoenrichment/ ..\data\raw\foursquare_geoenrichment\ --recursive
