from models.sentiment_model import SentimentModel

print("Think MVP pipeline started 🚀")

model = SentimentModel()
result = model.predict("Think MVP is becoming powerful!")

print(result)