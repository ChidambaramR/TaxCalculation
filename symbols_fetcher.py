import datetime
import json
import re
import time

import pandas as pd
import requests
from sqlalchemy import create_engine


def get_symbol(r):
    pattern = r"<span>(.*?)</span>"
    matches = re.findall(pattern, r)
    return matches


def fetch_symbols(scrip_codes):
    def get_details_from_database():
        file_path = 'stock_codes.db'
        database_url = f'sqlite:///{file_path}'
        engine = create_engine(database_url)
        sql_table_name = "codes"
        try:
            sql_df = pd.read_sql_table(sql_table_name, con=engine)
        except:
            sql_df = pd.DataFrame()

        return sql_df

    def put_details_to_database(df):
        if df.empty:
            return

        file_path = 'stock_codes.db'
        database_url = f'sqlite:///{file_path}'
        engine = create_engine(database_url)
        sql_table_name = "codes"
        df.to_sql(sql_table_name, con=engine, if_exists='append')

    codes_already_fetched = get_details_from_database()
    codes_to_be_fetched = scrip_codes
    print("Request to fetch codes {}".format(codes_to_be_fetched))

    if not codes_already_fetched.empty:
        codes_already_fetched = codes_already_fetched['code'].tolist()
        print("Codes already fetched {}".format(codes_already_fetched))
        codes_to_be_fetched = list(set(scrip_codes) - set(codes_already_fetched))

    print("Codes to be fetched {}".format(codes_to_be_fetched))
    code_to_symbols = []

    for code in codes_to_be_fetched:
        if code == "0000" or code == "000" or code == "208539" or code == "IN0020200286" or not (re.match("[0-9]+", code) or re.match("IN\d{10}", code)):
            print("Invalid code {}. Not fetching!".format(code))
            continue

        print('Fetching details for code {}'.format(code))
        url = "https://www.moneycontrol.com/mccode/common/autosuggestion_solr.php?classic=true&query={}&type=1&format=json&callback=suggest1".format(code)
        response = requests.get(url)
        time.sleep(5)

        if response.status_code == 200:
            print(response.text)
            code_to_symbol_map = {}
            response_json = response.text[10:-2]
            response_json = json.loads(response_json)

            if response_json['pdt_dis_nm'] == "No Result Available":
                print("No response for {}".format(code))
                continue

            symbol_details = get_symbol(response_json['pdt_dis_nm'])[0].split(' ')
            code_to_symbol_map['isin'] = re.sub(r"[^0-9a-zA-Z]+$", "", symbol_details[0])
            code_to_symbol_map['name'] = re.sub(r"[^a-zA-Z]+$", "", symbol_details[1])
            code_to_symbol_map['code'] = symbol_details[2]
            code_to_symbol_map['stock_name'] = response_json['stock_name']
            code_to_symbol_map['sector_id'] = response_json['sc_sector_id']
            code_to_symbol_map['sector'] = response_json['sc_sector']
            code_to_symbols.append(code_to_symbol_map)

    code_to_symbols_df = pd.DataFrame(code_to_symbols)
    put_details_to_database(code_to_symbols_df)


# fetch_symbols(["500209", "500102"])
