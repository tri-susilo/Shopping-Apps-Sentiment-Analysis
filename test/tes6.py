from sqlalchemy import create_engine
import pandas as pd

def insert_dataframe_to_postgresql(df, table_name, engine):
    # Membuat koneksi menggunakan SQLAlchemy
    conn = engine.connect()

    # Membaca data dari tabel ke dataframe untuk memeriksa reviewId yang sudah ada
    existing_reviewIds = pd.read_sql(f'SELECT "reviewId" FROM {table_name}', conn)['reviewId'].tolist()

    # Iterasi melalui dataframe dan masukkan data jika reviewId belum ada di database
    for index, row in df.iterrows():
        reviewId = row['reviewId']
        if reviewId not in existing_reviewIds:
            # Masukkan data ke tabel menggunakan SQLAlchemy
            conn.execute(f'INSERT INTO {table_name} ("reviewId", "kolom_lain") VALUES (%s, %s)', (reviewId, row['kolom_lain']))
            existing_reviewIds.append(reviewId)

    # Tutup koneksi
    conn.close()

# Contoh penggunaan
if __name__ == "__main__":
    # Ganti dengan informasi koneksi PostgreSQL Anda
    engine = create_engine('postgresql://postgres:postgres@localhost/test_sasa_db')

    # Ganti dengan nama tabel dan dataframe Anda
    table_name = "tes6"
    df = pd.DataFrame({
        "reviewId": [5, 6, 3, 1, 7],  # Contoh data dengan reviewId duplikat
        "kolom_lain": ["nilai1", "nilai2", "nilai3", "nilai1", "nilai4"]
    })

    insert_dataframe_to_postgresql(df, table_name, engine)

# engine = create_engine('postgresql://postgres:postgres@localhost/test_sasa_db')

# def get_data():
#     query = f"""
#     SELECT * FROM tes3;
#     """
#     return pd.read_sql(query, engine)

# df = pd.DataFrame(get_data())
# print(df.columns)

