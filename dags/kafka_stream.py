import uuid
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Mitesh Agarwal',
    'start_date': datetime(2025, 7, 21, 10, 0),
}

def get_data():
    
    import requests
    
    result = requests.get("https://www.randomuser.me/api/")
    result = result.json()
    
    result = result['results'][0]
    
    return result
    

def format_data(result):
    data = {}
    location = result['location']
    #data['id'] = uuid.uuid4()
    data['first_name'] = result['name']['first']
    data['last_name'] = result['name']['last']
    data['gender'] = result['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = result['email']
    data['username'] = result['login']['username']
    data['dob'] = result['dob']['date']
    data['registered_date'] = result['registered']['date']
    data['phone'] = result['phone']
    data['picture'] = result['picture']['medium']

    return data
    

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging
    
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()
    
    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            result = get_data()
            result = format_data(result)

            # Send the formatted data to Kafka topic 'users_created'
            producer.send('users_created', json.dumps(result).encode('utf-8'))
        
        except Exception as e:
            logging.error(f"Error while processing data: {e}")
            continue

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )