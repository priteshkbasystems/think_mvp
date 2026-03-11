from models.sentiment_model import SentimentModel

model = SentimentModel()

texts = [
    "แอพใช้งานง่ายมาก ดีขึ้นกว่าเดิม",
    "ระบบล่มบ่อยมาก ใช้งานไม่ได้",
    "เพิ่มเมนูชำระสินเชื่อแล้ว ดีมาก"
]

results = model.predict_batch(texts)

for t, r in zip(texts, results):
    print(t)
    print(r)
    print()