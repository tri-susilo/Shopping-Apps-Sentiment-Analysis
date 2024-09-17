
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers import pipeline
tokenizer = AutoTokenizer.from_pretrained("sahri/indonesiasentiment")

model = AutoModelForSequenceClassification.from_pretrained("sahri/indonesiasentiment")




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
  

