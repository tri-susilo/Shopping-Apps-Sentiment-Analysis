import pandas as pd
import re
from model import predict_sentimen
from test_get_data import _get_data_from_postgres,_get_data_from_postgress
from test_scrap import insert_to_database
from cleaner import remove_punctuation_and_emoji
import sqlalchemy
import numpy as np
from test_scrap import scrape_apps_review
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:postgres@localhost/test_sasa_db')

app_id_to_scrape = ['com.tokopedia.tkpd', 'com.shopee.id', 'com.lazada.android', 'blibli.mobile.commerce', 'com.bukalapak.android']
app_id_mapping = {
    'com.tokopedia.tkpd': 'tokopedia',
    'com.shopee.id': 'shopee',
    'com.lazada.android': 'lazada',
    'blibli.mobile.commerce' : 'blibli',
    'com.bukalapak.android' : 'bukalapak'

    # Add more mappings as needed
}

df_list = []

for app_id in app_id_to_scrape:
    # Perform some task for each app_id
    print(f"Scraping reviews for {app_id}")
    df = scrape_apps_review(app_id)

    # Map app_id to a custom name
    df['app_id'] = app_id_mapping.get(app_id, app_id)

    df_list.append(df)

final_df = pd.concat(df_list, ignore_index=True)
insert_to_database(df, db_table ="tes3", engine=engine)

print(final_df)

# if __name__ == '__name__':

#     #get data
#     df = pd.DataFrame(_get_data_from_postgres())

#     #clean data
#     #df["clean"] = df["content"].apply(remove_punctuation_and_emoji)
#     # df["clean_text"] = df["content"].apply(remove_punctuation_and_emoji)

#     print(df)

# df = pd.DataFrame(_get_data_from_postgress())
# column_to_drop = ['userName','userImage', 'content', 'score',
#        'thumbsUpCount', 'reviewCreatedVersion', 'at', 'replyContent',
#        'repliedAt', 'appVersion', 'app_id', 'date_fetch']
# df.drop(columns=column_to_drop, axis=1, inplace=True)
# # df.rename(columns={'userName':'sentiments'}, inplace=True)
# # df['sentiments']= None
# print(df)

# # df = pd.read_csv('reviewId1.csv')
# # # df.drop('content', axis=1, inplace=True)
# # df['content']= ''
# # df.rename(columns={'content':'sentiments'}, inplace=True)
# insert_to_database(df, db_table ="sentiment", engine=engine)

# app_id_to_scrape = 'com.tokopedia.tkpd'

# # Scrape app reviews
# df = scrape_apps_review(app_id_to_scrape)
# print(df)

