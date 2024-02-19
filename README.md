# VaxMap Thailand project
 
## Overview
The `VaxMap Thailand` is the personal project that aims to analyze and visualize the coverage of vaccines received by children aged 1 year in Thailand, as per the guidelines which consists of BCG, HB1, DTP-HB3, OPV3, IPV and MMR1. This project will leverage data primarily from the Ministry of Public Health's Open Data platform to evaluate vaccination coverage, map hospital locations, and provide insights through an interactive dashboard.

[The official dashboard for this data](https://hdcservice.moph.go.th/hdc/reports/report.php?cat_id=4df360514655f79f13901ef1181ca1c7&id=28dd2c7955ce926456240b2ff0100bde) was already done by Health Data Center (HDC) Service.

## Data Sources
This project utilizes data by

[Ministry of Public Health (MOPH) 's Open Data](https://opendata.moph.go.th/) from the following sources:

- Vaccination Coverage: "ความครอบคลุมการได้รับวัคซีนแต่ละชนิดครบตามเกณฑ์ในเด็กอายุครบ 1 ปี (fully immunized)" [Vaccination Coverage](https://opendata.moph.go.th/th/services/summary-table/4df360514655f79f13901ef1181ca1c7/s_epi_complete/28dd2c7955ce926456240b2ff0100bde).

- Hospital Coordinates: Essential for mapping and analysis [Hospital GIS](https://opendata.moph.go.th/th/services/hospital-gis).

- Map Service: Utilized for geospatial visualization. [Map Service](https://opendata.moph.go.th/th/services/map).

[Health Data Center (HDC), MOPH](https://dmd-ict.moph.go.th/main/download)

- `ShapefileThaiTableau` Folder for Tableau

- `ตัวอย่างข้อมูลรหัสสถานพยาบาล.csv` Sample of `hoscode` for Tableau

[Health Education Division, MOPH](https://healthgate.hss.moph.go.th/) for hospital data


## Objectives
- To assess the vaccination coverage among children aged 1 year for various vaccine types as per the set guidelines.

- To map the geographical distribution of hospitals in relation to the vaccination coverage data.

- To create an interactive dashboard for visualizing the data insights, aimed at both public health officials and the general public.

## Tools
- Python and Jupyter Notebook for data analysis and preprocessing.
    - `s_epi_complete.ipynb` show data description and how to use public API to load data into pandas DataFrame.
    - Folder `search_{}` for web scraping (ETL) by gazpacho library.

- Power BI or Tableau for dashboard creation and data visualization.