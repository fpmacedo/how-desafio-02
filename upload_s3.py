#%%
import s3_utils
# %%
bucket_name = 'how-desafio'


buckets = s3_utils.list_bucket()

if bucket_name in buckets:
    print(f"Bucket {bucket_name} allready exists.")
else:
    print("Creating bucket")
    s3_utils.create_bucket(bucket_name, 'us-east-2')

# %%

#create and upload orders

days = 32
orders = 200

for day in range(1, days):
    for i in range(orders):
        day_formatted = f"0{day}" if day<10 else day
        file_formatted = f"0{i}" if i<10 else i
        file_folder = (f"2023-05-{day_formatted}")
        file_name = (f"2023-05-{day_formatted}-{file_formatted}")
        file_path = f"order-data/{file_folder}/{file_name}.json"
        object_name = f"raw/orders/{file_folder}/{file_name}.json"
        s3_utils.upload_file(file_path, bucket_name, object_name)

#upload pyspark script
s3_utils.upload_file("spark_script.py", bucket_name, "spark/script.py")

# %%
