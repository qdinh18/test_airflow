import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import logging
import time
import random
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Configure logging
logging.basicConfig(level=logging.INFO)

# List of possible User-Agent strings to avoid detection
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
]

# Helper function to get random user-agent
def get_random_user_agent():
    return random.choice(USER_AGENTS)

# Helper function to introduce random delay between requests
def random_delay(min_time=2, max_time=5):
    delay = random.uniform(min_time, max_time)
    logging.info(f"Sleeping for {delay:.2f} seconds to mimic human behavior.")
    time.sleep(delay)

# Helper functions to extract book data
def get_title(soupp):
    try:
        title = soupp.find("span", attrs={"id": "productTitle"}).text.strip()
    except AttributeError:
        title = np.nan
    return title

def get_price(soupp):
    try:
        price = soupp.find('span', attrs={'class': 'a-size-base a-color-secondary'}).text.strip()
    except AttributeError:
        price = np.nan
    return price

def get_rate(soupp):
    try:
        rate = soupp.find('i', attrs={'class': 'a-icon a-icon-star a-star-4-5 cm-cr-review-stars-spacing-big'}).text.replace('out of 5 stars', '').replace(' ', '').strip()
    except AttributeError:
        rate = np.nan
    return rate

# Task 1: Extract Data from Amazon with retry mechanism for 503 errors
def extract(ti):
    logging.info("Starting data extraction...")

    dictt = {
        'title': [],
        'price': [],
        'rate': []
    }

    hd = {'User-Agent': get_random_user_agent(), 'Accept-Language': 'en-US, en;q=0.5'}
    url = 'https://www.amazon.com/s?k=data+engineering+books&ref=nb_sb_noss'
    re = requests.get(url, headers=hd)

    # Retry mechanism if 503 error occurs
    retries = 5
    backoff_time = 2
    while re.status_code == 503 and retries > 0:
        logging.warning(f"Received 503 error. Retrying... {retries} retries left.")
        time.sleep(backoff_time)  # Exponential backoff
        backoff_time *= 2  # Increase the delay time for the next retry
        hd['User-Agent'] = get_random_user_agent()  # Rotate User-Agent
        re = requests.get(url, headers=hd)
        retries -= 1

    if re.status_code != 200:
        logging.error(f"Failed to retrieve data from Amazon after retries. Status Code: {re.status_code}")
        return

    html_doc = re.text
    soup = BeautifulSoup(html_doc, 'html.parser')
    links = soup.find_all('a', attrs={'class': 'a-link-normal s-no-outline'})
    links_list = [link.get('href') for link in links]

    logging.info(f"Found {len(links_list)} links. Starting to extract individual book data...")

    for link in links_list:
        # Introduce delay before requesting each book page
        random_delay(2, 5)  # Delay between 2 to 5 seconds

        res = requests.get(f'https://www.amazon.com{link}', headers=hd)

        if res.status_code == 503:
            logging.warning("Received 503 error on book link. Skipping this link.")
            continue

        soupp = BeautifulSoup(res.text, 'html.parser')
        dictt['title'].append(get_title(soupp))
        dictt['price'].append(get_price(soupp))
        dictt['rate'].append(get_rate(soupp))

        logging.info(f"Extracted: Title: {dictt['title'][-1]}, Price: {dictt['price'][-1]}, Rate: {dictt['rate'][-1]}")

    logging.info("Data extraction completed.")
    ti.xcom_push(key='books_dict', value=dictt)

# Task 2: Transform Data (No changes here)
def transform(ti):
    logging.info("Starting data transformation...")
    books_dict = ti.xcom_pull(key='books_dict', task_ids='extract_data')
    
    if not books_dict:
        logging.error("No data found from extract task!")
        return
    
    df = pd.DataFrame(books_dict)
    df.drop_duplicates(inplace=True)
    logging.info(f"Data after transformation: \n{df.head()}")
    
    ti.xcom_push(key='books_dict', value=df.to_dict('records'))

# Task 3: Load Data into PostgreSQL (No changes here)
def load(ti):
    logging.info("Starting data load into PostgreSQL...")
    books_dict = ti.xcom_pull(key='books_dict', task_ids='transform_data')
    
    if not books_dict:
        logging.error("No data found from transform task!")
        return
    
    postgres_hook = PostgresHook(postgres_conn_id='book_connection')
    insert_query = '''
        INSERT INTO Books (title, price, rate)
        VALUES (%s, %s, %s)
    '''
    
    for book in books_dict:
        logging.info(f"Inserting book: {book['title']}")
        postgres_hook.run(insert_query, parameters=(book['title'], book['price'], book['rate']))

    logging.info("Data load completed.")

# Default args and DAG definition
default_args = {
    'owner': 'quyendinh',
    'depends_on_past': False,
    'email': ['dinhvietquyen1803@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 9, 26)
}

dag = DAG('fetch_and_store', default_args=default_args, schedule_interval=timedelta(days=1))

# Define tasks in the DAG
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform,
    dag=dag
)

store_task = PythonOperator(
    task_id='store_data',
    python_callable=load,
    dag=dag
)

# Task dependencies
extract_task >> transform_task >> store_task
