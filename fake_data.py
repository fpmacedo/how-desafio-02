# %%
from faker import Faker
import json
import numpy as np
import uuid
import datetime
import os
# %%
fake = Faker('pt_BR')

products = ['SSD Externo Samsung',
            'Placa de Vídeo RTX 3060',
            'Mouse Gamer HyperX',
            'Monitor Gamer Asus',
            'Tablet Samsung Galaxy',
            'Memória Gamer Husky',
            'Smartwatch Hit',
            'Smart TV Samsung',
            'HD Toshiba L200',
            'Microfone HyperX',
            'Teclado e Mouse Microsoft']

#%%
products_id = []

for i in range(11):
   id = str(uuid.uuid4())
   products_id.append(id)

#%%

products_price = [29990,
                  350050,
                  13000,
                  149999,
                  59999,
                  8990,
                  70000,
                  259980,
                  34290,
                  86089,
                  20000]

#%%


days = 32
orders = 200
for day in range(1, days):
    orders_data = []
    for i in range(orders):
        random_product = np.random.randint(0,10)
        dt_ini = datetime.datetime(2023, 5, (day))
        dt_end = datetime.datetime(2023, 5, (day)) + datetime.timedelta(days=1)
        data = {
                "order_id": str(uuid.uuid1()),
                "order_total": products_price[random_product],
                "product_info":{
                                "product_id": products_id[random_product],
                                "product_description": products[random_product],
                                },            
                "customer_info":
                                {
                                 "customer_id": str(uuid.uuid4()),
                                 "customer_name": fake.name(),
                                 "customer_phone_number": fake.phone_number()
                                 },
                "payment_info": {
                                 "payment_type": "credit_card",
                                 "card_number": fake.credit_card_number(),
                                 "provider": fake.credit_card_provider(),
                                 "expire": fake.credit_card_expire(),
                                 "security_code": fake.credit_card_security_code(),
                                },
                "delivery_address": {
                                        "zip_code": fake.postcode(formatted=False),
                                        "country": fake.current_country(),
                                        "state": fake.estado_nome(),
                                        "city": fake.city(),
                                        "street": fake.street_name(),
                                        "number": fake.building_number()
                                    },
                "order_created_at": str(fake.date_time_between_dates(dt_ini, dt_end))
                }

        # Serializing json
        json_object = json.dumps(data, ensure_ascii=False)
        day_formatted = f"0{day}" if day<10 else day
        file_formatted = f"0{i}" if i<10 else i
        file_folder = (f"2023-05-{day_formatted}")
        file_name = (f"2023-05-{day_formatted}-{file_formatted}")
        file_path = f"order-data/{file_folder}/{file_name}.json"
        # Writing to sample.json
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(f"order-data/{file_folder}/{file_name}.json", "w", encoding='utf8') as outfile:
            outfile.write(json_object)
