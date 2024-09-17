import pandas as pd
import re
from model import predict_sentimen
from test_get_data import _get_data_from_postgres
from test_scrap import insert_to_database
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:postgres@localhost/test_sasa_db')

def remove_punctuation_and_emoji(text):
    # Remove punctuation using regular expression
    text = re.sub(r'[^\w\s]', '', text)
    
    # Remove emojis using regular expression
    text = re.sub(r'[\U00010000-\U0010ffff]', '', text)

    text = ' '.join(text.split())
    text = re.sub(r'(.)\1+', r'\1', text)
    
    return text

# retrive data from database
#data = _get_data_from_postgres() 
# print(data)
# df = pd.DataFrame(_get_data_from_postgres())
# print(df)

if __name__ == '__main__':

    #retrive the data
    df = pd.DataFrame(_get_data_from_postgres())

    #Clean the data
    df["clean_text"] = df["content"].apply(remove_punctuation_and_emoji)

    #predict the sentence
    df["sentiments"] = df["clean_text"].apply(predict_sentimen)

    #drop column content dan clean text
    columns_to_drop = ["content","clean_text", "clean_text"]
    df.drop(columns_to_drop, axis=1, inplace=True)

    #insert to table "sentiment"
    insert_to_database(df, db_table ="sentiment", engine=engine)

    print(df)


# #Create an example DataFrame
# data = {
#     'ID': [1, 2, 3,4],
#     'Text': [
#         "Hello! 😊 kamu seperti babi! 😎",
#         "aplikasinya bagus sekali   ! 😃",
#         "bagusss",
#         "Sementara bintang 3 dulu. mitra Expedisi tokopedia (JNE) selalu dan selalu mengecewakan. edited : jadi bintang 1. Karna jawabanyya robot. Mau saran malah disuruh hubungi tokopedia care."
#     ]
# }

# df = pd.DataFrame(data)

# # Apply the cleaning function to the 'Text' column
# df['Cleaned_Text'] = df['Text'].apply(remove_punctuation_and_emoji)
# print(df)
# df['sentimen'] = df['Cleaned_Text'].apply(predict_sentimen)


# # Display the DataFrame
# print(df)