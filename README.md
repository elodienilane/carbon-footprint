# carbon-footprint
Fetch **Global Carbon Footprint** data using the Global Footprint Network API and save as .csv for **Snowflake** copy-into.

## Full architecture
![pam-carbon-footprint (7)](https://user-images.githubusercontent.com/1748668/221438730-bc347766-f62c-4e27-83b8-d14bb32d5881.jpg)

## Global Footprint Network API
Available at https://data.footprintnetwork.org/#/api (see documentation for endpoints).
API requires credentials that should be stored in **AWS Secrets Manager** (stored in constants.py for the moment).

## AWS S3
Two buckets configured in s3:
- gfn-raw for Lambda [2] to upload data fresh from the API
- gfn-transformed for Lambda [4] to upload transformed data in csv format, for snowflake

![image](https://user-images.githubusercontent.com/1748668/221438437-a49ac438-8673-4df5-bcb2-c0251d1f19a7.png)

## AWS Lambdas
### gfn-fetch
Get data from API, with write permission on gfn-raw bucket.

<img width="1213" alt="image" src="https://user-images.githubusercontent.com/1748668/221517618-3ee1ed17-c5d9-4b97-85a4-5cffaed9a468.png">

![image](https://user-images.githubusercontent.com/1748668/221868895-354dfb16-534b-430d-9390-7fd1be744b2b.png)

### gfn-etl
Triggered by data upload to gfn-raw bucket, extract from file, transform, load as csv to gfn-transformed bucket. Read permission on gfn-raw, write permission on gfn-transformed.

<img width="1214" alt="image" src="https://user-images.githubusercontent.com/1748668/221517686-73c25bba-d2ae-4459-961b-c83abc53a596.png">

![image](https://user-images.githubusercontent.com/1748668/221868074-dd28b460-7e73-479b-b82b-a38a1c299588.png)

Layers:
![image](https://user-images.githubusercontent.com/1748668/221868533-9ee6f940-988d-4248-a6d5-d9c208ed6fd4.png)



## To run your own copy:
- Add API credentials in constants.py
- Install requirements using pip install -r requirements.txt
- Set the mode in constants.py (mode = "FAKE" for local file upload, mode = "PROD" for s3 file upload)
