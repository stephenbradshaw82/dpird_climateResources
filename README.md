# dpird_climateResources

## Introduction
This repo provides a series of scripts and functions to:

(i) Initially save out source netcdf resources as .rds files (initially from CMPI6 and BRAN2020)
	- prevents all users storing similar files outside of a wider organisational location
	
(ii) the wrangling of these files (from their respective directories)
	- filtering further based on parameter, time and spatial location 


## Files
```plaintext
Parent Folder
├── 00_src
│   └── functions.R
├── 01_scripts
│   ├── 000_Source_NetCDF_to_rds.R
│   ├── 100_BRAN_data.R
│   └── 200_CMIP6_data.R
├── 02_netCDFrds_BRAN
│   ├── rds_file1
│   ├── rds_file2
│   ├── ...
│   └── rds_fileN
└── 03_netCDFrds_outputs_CMIP6
    ├── rds_file1
    ├── rds_file2
    ├── ...
    └── rds_fileN
```