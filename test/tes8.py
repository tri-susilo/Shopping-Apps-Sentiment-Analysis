import pandas as pd
from sqlalchemy import create_engine

def update_sentiments_in_postgresql(data_frame):
    # Ganti dengan informasi koneksi database Anda
    db_connection = 'postgresql://postgres:postgres@localhost/test_sasa_db'
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

# Contoh penggunaan fungsi
df = pd.read_csv('coba.csv')
update_sentiments_in_postgresql(df)
