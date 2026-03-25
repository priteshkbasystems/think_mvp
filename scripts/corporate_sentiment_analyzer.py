import re
import numpy as np
from models.sentiment_model import SentimentModel
from sentence_transformers import SentenceTransformer


def detect_thai(text):
    return any("\u0E00" <= ch <= "\u0E7F" for ch in text)


def split_sentences(text):
    text = str(text).strip()
    if not text:
        return []
    if detect_thai(text):
        try:
            from pythainlp.tokenize import sent_tokenize as thai_sent_tokenize
            sentences = thai_sent_tokenize(text)
        except ImportError:
            sentences = re.split(r"(?<=[.!?।])\s*", text)
    else:
        sentences = re.split(r"(?<=[.!?।])\s*", text)
    return [s.strip() for s in sentences if len(s.strip()) > 5]


def classify_utterance_kind(sentence):
    s = sentence.lower()

    en_claim = [
        "will",
        "we aim",
        "we plan",
        "we commit",
        "ensure",
        "promise",
        "guarantee",
        "going to",
    ]
    th_claim = ["จะ", "สัญญา", "มีแผน", "มุ่งมั่น", "ประกัน"]
    hi_claim = ["करेंगे", "वादा", "योजना", "सुनिश्चित", "करूँगा", "करेंगी"]

    if any(x in s for x in en_claim + th_claim + hi_claim):
        return "claim"

    en_fulfillment = [
        "have delivered",
        "has improved",
        "achieved",
        "completed",
        "successfully",
        "fixed",
        "implemented",
    ]
    th_fulfillment = ["สำเร็จ", "บรรลุ", "เสร็จสิ้น", "แก้ไขแล้ว", "พัฒนาแล้ว"]
    hi_fulfillment = ["किया", "सुधार", "पूरा", "सफल", "निश्चित रूप से"]

    if any(x in s for x in en_fulfillment + th_fulfillment + hi_fulfillment):
        return "fulfillment"

    return "statement"


def to_signed_score(label, score):
    score = float(score)
    if label == "POSITIVE":
        return score
    if label == "NEGATIVE":
        return -score
    return 0.0


def bin_label(signed_value, pos_threshold, neg_threshold):
    if signed_value > pos_threshold:
        return "positive"
    if signed_value < neg_threshold:
        return "negative"
    return "neutral"


class CorporateSentimentAnalyzer:

    def __init__(
        self,
        sentence_pos_threshold=0.05,
        sentence_neg_threshold=-0.05,
        page_pos_threshold=0.05,
        page_neg_threshold=-0.05,
        document_pos_threshold=0.05,
        document_neg_threshold=-0.05,
    ):

        print("Loading Corporate Sentiment Analyzer...")

        self.sentiment_model = SentimentModel()
        self.embedder = SentenceTransformer("all-MiniLM-L6-v2")

        self.sentence_pos_threshold = sentence_pos_threshold
        self.sentence_neg_threshold = sentence_neg_threshold
        self.page_pos_threshold = page_pos_threshold
        self.page_neg_threshold = page_neg_threshold
        self.document_pos_threshold = document_pos_threshold
        self.document_neg_threshold = document_neg_threshold

        self.topics = [
            "digital transformation",
            "mobile banking app",
            "customer service",
            "ai automation",
            "payment systems",
            "security",
            "user experience",
            "operations efficiency",
        ]

        self.topic_embeddings = self.embedder.encode(self.topics)

    def classify_topic(self, sentence):

        emb = self.embedder.encode([sentence])[0]

        scores = np.dot(self.topic_embeddings, emb)

        best_idx = int(np.argmax(scores))

        return self.topics[best_idx]

    def analyze_pages(self, pages):
        """
        pages: list of (page_number: int, text: str)
        Returns topics, sentences, pages rollups, document rollup.
        """
        sentences_out = []
        topic_scores = {}
        sentence_index = 0

        for page_number, page_text in pages:
            sents = split_sentences(page_text)
            if not sents:
                continue

            sentiments = self.sentiment_model.predict_batch(sents)

            for sent, sentiment in zip(sents, sentiments):
                topic = self.classify_topic(sent)
                label = sentiment["label"]
                conf = sentiment["score"]
                signed = to_signed_score(label, conf)
                utterance_kind = classify_utterance_kind(sent)
                slabel = bin_label(
                    signed,
                    self.sentence_pos_threshold,
                    self.sentence_neg_threshold,
                )

                if topic not in topic_scores:
                    topic_scores[topic] = []
                topic_scores[topic].append(signed)

                sentences_out.append(
                    {
                        "sentence_index": sentence_index,
                        "page_number": page_number,
                        "sentence": sent,
                        "sentiment_label": label,
                        "sentiment_score": float(conf),
                        "signed_score": signed,
                        "utterance_kind": utterance_kind,
                        "topic": topic,
                        "label": slabel,
                    }
                )
                sentence_index += 1

        topics = {t: float(np.mean(scores)) for t, scores in topic_scores.items()}

        pages_out = []
        if sentences_out:
            by_page = {}
            for row in sentences_out:
                pn = row["page_number"]
                if pn not in by_page:
                    by_page[pn] = []
                by_page[pn].append(row["signed_score"])

            for pn in sorted(by_page.keys()):
                scores_p = by_page[pn]
                mean_p = float(np.mean(scores_p))
                plab = bin_label(
                    mean_p,
                    self.page_pos_threshold,
                    self.page_neg_threshold,
                )
                pages_out.append(
                    {
                        "page_number": pn,
                        "mean_signed": mean_p,
                        "sentence_count": len(scores_p),
                        "label": plab,
                    }
                )

        doc_mean = 0.0
        doc_label = "neutral"
        if pages_out:
            doc_mean = float(np.mean([p["mean_signed"] for p in pages_out]))
            doc_label = bin_label(
                doc_mean,
                self.document_pos_threshold,
                self.document_neg_threshold,
            )

        return {
            "topics": topics,
            "sentences": sentences_out,
            "pages": pages_out,
            "document": {"doc_mean_signed": doc_mean, "label": doc_label},
        }

    def analyze_document(self, text):
        """Single block as page 1; returns same structure as analyze_pages."""
        return self.analyze_pages([(1, text)])
