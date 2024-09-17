from sqlalchemy import create_engine
import pandas as pd

from sqlalchemy import create_engine
import pandas as pd

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

    # Iterasi melalui dataframe dan masukkan data jika reviewId belum ada di database
    for index, row in df.iterrows():
        reviewId = row['reviewId']
        if reviewId not in existing_reviewIds:
            # Ambil nilai untuk setiap kolom yang akan diisi
            values = [row[col] for col in columns_to_insert]

            # Masukkan data ke tabel menggunakan SQLAlchemy
            insert_query = f"""INSERT INTO {table_name} ("reviewId", "userName", "userImage", "content", "score", "thumbsUpCount", "reviewCreatedVersion", "at", "replyContent", "repliedAt", "appVersion", "app_id", "date_fetch") VALUES ({', '.join(['%s'] * len(columns_to_insert))})"""
            conn.execute(insert_query, values)
            existing_reviewIds.append(reviewId)

    # Tutup koneksi
    conn.close()

#Contoh penggunaan
if __name__ == '__main__':
    # Ganti dengan informasi koneksi PostgreSQL Anda
    engine = create_engine('postgresql://postgres:postgres@localhost/test_sasa_db')

    # Ganti dengan nama tabel dan dataframe Anda
    table_name = "tes7"
    df = pd.DataFrame({
        "reviewId": [5, 6, 2, 1, 7],  # Contoh data dengan reviewId duplikat
        "userName": ['User1', 'User2', 'User3', 'User1', 'User4'],
        "userImage": ['Image1', 'Image2', 'Image3', 'Image1', 'Image4'],
        "content": ['Content1', 'Content2', 'Content3', 'Content1', 'Content4'],
        "score": [5, 4, 5, 5, 4],
        "thumbsUpCount": [10, 8, 12, 9, 7],
        "reviewCreatedVersion": ['1.0', '2.0', '3.0', '1.0', '2.0'],
        "at": ['Time1', 'Time2', 'Time3', 'Time1', 'Time4'],
        "replyContent": ['Reply1', 'Reply2', 'Reply3', 'Reply1', 'Reply4'],
        "repliedAt": ['ReplyTime1', 'ReplyTime2', 'ReplyTime3', 'ReplyTime1', 'ReplyTime4'],
        "appVersion": ['AppVersion1', 'AppVersion2', 'AppVersion3', 'AppVersion1', 'AppVersion4'],
        "app_id": [101, 102, 103, 101, 104],
        "date_fetch": ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-01', '2023-01-04']
    })

    insert_dataframe_to_postgresql(df, table_name, engine)

# engine = create_engine('postgresql://postgres:postgres@localhost/test_sasa_db')

# def get_data():
#     query = f"""
#     SELECT "reviewId" FROM tes7;
#     """
#     return pd.read_sql(query, engine)

# print(get_data())
