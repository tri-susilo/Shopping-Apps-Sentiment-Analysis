import pandas as pd
from google_play_scraper import reviews,Sort
from sqlalchemy import create_engine
from tqdm import tqdm
import time
from sqlalchemy import exc
import re
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers import pipeline
tokenizer = AutoTokenizer.from_pretrained("sahri/indonesiasentiment")
model = AutoModelForSequenceClassification.from_pretrained("sahri/indonesiasentiment")

#engine = create_engine('postgresql://postgres:postgres@localhost/test_sasa_db')
engine = create_engine('postgresql://username:password@endpoint/db_name')

# function to retrive data from from Google Play Store
def scrape_apps_review(app_id, n_max=2000):
    max_count_per_fetch = 100
    continuation_token = None
    results = [] # variable to keep final data

    p_bar = tqdm(total=n_max + max_count_per_fetch) # progress bar

    star_time = time.time() # record the start time
    while len(results) <= n_max:
        try:
            result, continuation_token = reviews(
                app_id,
                count=max_count_per_fetch,
                continuation_token=continuation_token,
                lang="id",  # defaults to 'en'
                country="id",  # defaults to 'us'
                sort=Sort.NEWEST,  # defaults to Sort.MOST_RELEVANT
                filter_score_with=None,  # defaults to None(means all score)
            )
        except:
            continue

        results += result
        p_bar.update(len(results))

        if continuation_token.token is None:
            break


    end_time = time.time() #record the end time
    elapsed_time = end_time - star_time

    df = pd.DataFrame(results)

    # Add 'app_id' and 'date_fetch' columns
    df['app_id'] = app_id
    df['date_fetch'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"Data retrieval completed in {elapsed_time:.2f} seconds")
    return df

# insert ke database tanpa duplikat
def insert_dataframe_to_postgresql(df, table_name, engine):
    # Membuat koneksi menggunakan SQLAlchemy
    conn = engine.connect()

    # Membaca data dari tabel ke dataframe untuk memeriksa reviewId yang sudah ada
    existing_reviewIds = pd.read_sql(f'SELECT "reviewId" FROM {table_name}', conn)['reviewId'].tolist()

    # Kolom-kolom yang akan diisi
    columns_to_insert = [
        'reviewId', 'userName', 'userImage', 'content', 'score',
        'thumbsUpCount', 'reviewCreatedVersion', 'at',
        'replyContent', 'repliedAt', 'appVersion', 'app_id', 'date_fetch'
    ]
    df = df.where(pd.notna(df), None)
    # Iterasi melalui dataframe dan masukkan data jika reviewId belum ada di database
    for index, row in df.iterrows():
        reviewId = row['reviewId']
        if reviewId not in existing_reviewIds:

            row['repliedAt'] = None if pd.isna(row['repliedAt']) else row['repliedAt']
            # Ambil nilai untuk setiap kolom yang akan diisi
            values = [row[col] for col in columns_to_insert]

            # Masukkan data ke tabel menggunakan SQLAlchemy
            insert_query = f"""INSERT INTO {table_name} ("reviewId", "userName", "userImage", "content", "score", "thumbsUpCount", "reviewCreatedVersion", "at", "replyContent", "repliedAt", "appVersion", "app_id", "date_fetch") VALUES ({', '.join(['%s'] * len(columns_to_insert))})"""
            conn.execute(insert_query, values)
            existing_reviewIds.append(reviewId)

    # Tutup koneksi
    conn.close()

def update_sentiments_in_postgresql(data_frame):
    # Ganti dengan informasi koneksi database Anda
    db_connection = 'postgresql://postgres:pisanggoreng123@sasa-db-instance.cto8fqajoc9d.ap-southeast-1.rds.amazonaws.com/sasa-db-prod'
    engine = create_engine(db_connection)
    
    # Loop melalui DataFrame dan perbarui data di database sesuai kecocokan "reviewId"
    for index, row in data_frame.iterrows():
        review_id = row['reviewId']
        sentiments = row['sentiments']

        # Query SQL untuk memperbarui kolom 'sentiments' di database sesuai 'reviewId'
        update_query = f"""UPDATE sentiment SET "sentiments" = %s WHERE "reviewId" = %s"""
        
        # Eksekusi query dengan menggunakan engine SQLAlchemy
        with engine.connect() as connection:
            connection.execute(update_query, (sentiments, review_id))

# function to retrive data from database
def _get_data_from_postgres( n=1000):
    query = f"""
    SELECT "review"."reviewId",
    "review"."content" FROM review
    LEFT JOIN sentiment
    ON ("review"."reviewId"="sentiment"."reviewId")
    WHERE "sentiment"."sentiments" IS NULL;
    """
    return pd.read_sql(query, engine)

# function to cleaning the text
def remove_punctuation_and_emoji(text):
    if text is None:
        return None
    try:
        # Remove punctuation using regular expression
        text = re.sub(r'[^\w\s]', '', text)
        
        # Remove emojis using regular expression
        text = re.sub(r'[\U00010000-\U0010ffff]', '', text)

        text = ' '.join(text.split())
        text = re.sub(r'(.)\1+', r'\1', text)
        
        return text
    except Exception as e:
        return None
# function to call the model 
nlp = pipeline(
    "sentiment-analysis",
    model=model,
    tokenizer=tokenizer
)
def predict_sentimen(text):
  if text:
    a = nlp(text)
    return a[0]["label"]
  else:
    return "neutral"
  
if __name__ == '__main__':

    star_time = time.time() # record the start time

    #retive data from Google Play Store
    app_id_to_scrape = ['com.tokopedia.tkpd','com.shopee.id','com.lazada.android','blibli.mobile.commerce','com.bukalapak.android']
    app_id_mapping = {
    'com.tokopedia.tkpd': 'tokopedia',
    'com.shopee.id': 'shopee',
    'com.lazada.android': 'lazada',
    'blibli.mobile.commerce' : 'blibli',
    'com.bukalapak.android' : 'bukalapak'
    }
    df_list = []

    for app_id in app_id_to_scrape:
    # Perform some task for each app_id
        print(f"Scraping reviews for {app_id}")
        df = scrape_apps_review(app_id)
        df['app_id'] = app_id_mapping.get(app_id, app_id)
        df_list.append(df)
    final_df = pd.concat(df_list, ignore_index=True)

    #insert data to database
    #insert_to_database(final_df, db_table ="tes3", engine=engine)
    insert_dataframe_to_postgresql(final_df, table_name="review", engine=engine)

    # retrive data from database
    df = pd.DataFrame(_get_data_from_postgres())
    print("done retrive data")

    #Clean the data
    df["clean_text"] = df["content"].apply(remove_punctuation_and_emoji)
    print("done cleaning data")
    print("begin labeling data")
    #predict the sentence
    df["sentiments"] = df["clean_text"].apply(predict_sentimen)

    #drop column content dan clean text
    columns_to_drop = ["content","clean_text"]
    df.drop(columns_to_drop, axis=1, inplace=True)

    #insert to table "sentiment"
    update_sentiments_in_postgresql(df)

    end_time = time.time() #record the end time
    elapsed_time = end_time - star_time

    print(f"Code completed in {elapsed_time:.2f} seconds")

    