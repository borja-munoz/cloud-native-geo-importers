# Geospatial data importers for cloud native data platforms

This repository contains Python scripts for importing geospatial data in cloud native data platforms.

## Redshift importer (geo2rs.py)

This script makes use of the following packages:

- [AWS SDK for Python](https://aws.amazon.com/sdk-for-python/)(Boto3) for interacting with AWS services (S3 and Redshift)
- [Fiona](https://github.com/Toblerity/Fiona) for reading geospatial data files
- [Shapely](https://github.com/Toblerity/Shapely) for writing EWKB geometries 

The main steps are:

1. Transform the input file to a CSV file with EWKB geometries
2. Upload the CSV file to a S3 bucket
3. Load the data in the CSV file into Redshift using the COPY command with the Redshift Data API

You need to install the [AWS CLI](https://aws.amazon.com/cli/) and configure your Access key ID, Secret access key and AWS Region where your Redshift cluster and S3 bucket are located. The Access key ID and Secret access key parameters will be used for authorizing the S3 upload operation and the access to the [Redshift Data API](https://docs.aws.amazon.com/redshift-data/latest/APIReference).

The script is built for Python 3 and it is recommended to create a virtual environment in the folder where the script is located and install the required Python packages:

```shell
python3 -m venv /path/to/script/folder
cd /path/to/script/folder
source bin/activate
pip install boto3
pip install fiona
pip install shapely
```

The script can be executed standalone or used as a module from another script/program. It requires the following parameters:

| Parameter  | Description                                      |
|------------|--------------------------------------------------|
| input_file | Input geospatial file. Supported formats: any file format with reading support in Fiona, including Esri Shapefile, GeoPackage, GeoJSON |
| bucket     | S3 bucket where the file will be uploaded        |
| cluster_identifier  | Redshift cluster identifier             |
| database   | Database where the data will be imported         |
| secret_arn | ARN of the secret that provides access to the database |
| redshift_role | ARN of the Redshift role with read access to S3 |
| table_name | Redshift table where the data will be imported. The script will error out if the table already exists |
