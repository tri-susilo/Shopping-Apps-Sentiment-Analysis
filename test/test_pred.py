from model import predict_sentimen
from cleaner import remove_punctuation_and_emoji
import pandas as pd


#Create an example DataFrame
data = {
    'ID': [1, 2, 3,4],
    'Text': [
        "Hello! 😊 kamu seperti babi! 😎",
        "aplikasinya bagus sekali   ! 😃",
        "",
        "Sementara bintang 3 dulu. mitra Expedisi tokopedia (JNE) selalu dan selalu mengecewakan. edited : jadi bintang 1. Karna jawabanyya robot. Mau saran malah disuruh hubungi tokopedia care."
    ]
}

df = pd.DataFrame(data)
# print(df)

# Apply the cleaning function to the 'Text' column
df['Cleaned_Text'] = df['Text'].apply(remove_punctuation_and_emoji)
# print(df)
df['sentimen'] = df['Cleaned_Text'].apply(predict_sentimen)
print(df)