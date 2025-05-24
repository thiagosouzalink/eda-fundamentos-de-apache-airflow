import requests
import logging

from datetime import datetime
from airflow.decorators import dag, task
from airflow.sdk import get_current_context

# list of cryptocurrencies to fetch (input for dynamic task mapping)
cryptocurrencies = ["ethereum", "dogecoin", "bitcoin", "tether", "tron"]
api_call_template = "https://api.coingecko.com/api/v3/simple/price?ids={crypto}&vs_currencies=usd"


@dag(
    dag_id="dtm_cryptocurrency_prices",
    schedule="@daily",
    start_date=datetime(2025, 5, 24),
    catchup=False
)
def crypto_prices():

    # task to dynamically map over the list of cryptocurrencies with custom index name
    @task(map_index_template="{{ crypto }}")
    def fetch_price(crypto: str):
        """Fetch the price of a given cryptocurrency from the API."""

        # use the cryptocurrency name in the task name
        context = get_current_context()
        context["crypto"] = crypto

        #  API call to fetch the price of the cryptocurrency
        api_url = api_call_template.format(crypto=crypto)
        response = requests.get(api_url).json()
        price = response[crypto]['usd']
        logging.info(f"the price of {crypto} is ${price}")

        return price

    #  dynamically map the fetch_price task over the list of cryptocurrencies
    prices = fetch_price.partial().expand(crypto=cryptocurrencies)

    prices


#  instantiate the DAG
crypto_prices()