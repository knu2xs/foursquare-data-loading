SETLOCAL
SET PROJECT_DIR=%~dp0..\

del /f/q/s ..\data\raw\foursquare_qa\*.* > null
rmdir /q/s ..\data\raw\foursquare_qa`\

aws s3 cp s3://esri-data-main/processed/delivery/foursquare_qa/parquet/ ..\data\raw\foursquare_qa\parquet\ --recursive
aws s3 cp s3://esri-data-main/processed/delivery/foursquare_qa/schema/ ..\data\raw\foursquare_qa\schema --recursive

del /f/q/s ..\data\raw\foursquare_qa_sample\*.* > null
rmdir /q/s ..\data\raw\foursquare_qa_sample`\

aws s3 cp s3://esri-data-main/processed/delivery/foursquare_qa_sample/ ..\data\raw\foursquare_qa_sample\ --recursive
