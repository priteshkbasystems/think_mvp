from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import torch.nn.functional as F


class SentimentModel:

    def __init__(self):

        model_name = "cardiffnlp/twitter-xlm-roberta-base-sentiment"

        print("Loading Sentiment Model:", model_name)

        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)

        self.labels = ["NEGATIVE", "NEUTRAL", "POSITIVE"]

    def predict(self, text):

        encoded = self.tokenizer(
            text,
            return_tensors="pt",
            truncation=True,
            padding=True,
            max_length=512
        )

        with torch.no_grad():
            output = self.model(**encoded)

        scores = F.softmax(output.logits, dim=1)[0]

        neg = scores[0].item()
        neu = scores[1].item()
        pos = scores[2].item()

        if pos >= neg and pos >= neu:
            label = "POSITIVE"
            score = pos

        elif neg >= pos and neg >= neu:
            label = "NEGATIVE"
            score = neg

        else:
            label = "NEUTRAL"
            score = neu

        return {
            "label": label,
            "score": score
        }

    def predict_batch(self, texts):

        results = []

        for text in texts:
            sentiment = self.predict(text)
            results.append(sentiment)

        return results