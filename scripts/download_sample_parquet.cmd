SETLOCAL
SET PROJECT_DIR="%~dp0..\"

del /f/q/s ..\data\raw\sample\*.* > null
rmdir /q/s ..\data\raw\sample\

aws s3 cp s3://esri-data-main/temp/venue_reality_sample/processed/sample/ ..\data\raw\sample\ --recursive