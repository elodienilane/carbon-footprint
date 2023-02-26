# carbon-footprint
Fetch Global Carbon Footprint data using the API provided at https://data.footprintnetwork.org/#/api and save as .csv for Snowflake copy-into

Full architecture:
![pam-carbon-footprint (7)](https://user-images.githubusercontent.com/1748668/221438730-bc347766-f62c-4e27-83b8-d14bb32d5881.jpg)

Two buckets configured in s3:
- gfn-raw for Lambda [2] to upload data fresh from the API
- gfn-transformed for Lambda [4] to upload transformed data in csv format, for snowflake

![image](https://user-images.githubusercontent.com/1748668/221438437-a49ac438-8673-4df5-bcb2-c0251d1f19a7.png)


To run your own copy:
- Add API credentials in constants.py
- Install requirements using pip install -r requirements.txt
- Set the mode in constants.py (mode = "FAKE" for local file upload, mode = "PROD" for s3 file upload)
