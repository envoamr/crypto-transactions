import kagglehub
import pandas as pd
from datetime import datetime

DATASET_NAME = "novandraanugrah/bitcoin-historical-datasets-2018-2024"
FREQUENCY = "{{ render(vars.btc_prices_frequency) }}"
INPUT_DATE = "{{ render(vars.input_date) }}"
API_FILE_NAME = f"btc_{FREQUENCY}_data_2018_to_2025.csv"
OUTPUT_FILE_NAME = "{{ render(vars.btc_prices_file) }}"

btc_prices_csv = kagglehub.dataset_download(DATASET_NAME, API_FILE_NAME)
df_btc_prices = pd.read_csv(btc_prices_csv)
df_btc_prices_date = df_btc_prices[df_btc_prices['Open time'].str.contains(INPUT_DATE)]
df_btc_prices_date.to_parquet(OUTPUT_FILE_NAME)