from scripts.conversation_sentiment_flow import ConversationSentimentFlow


def main():

    engine = ConversationSentimentFlow()

    example_chat = [
        "My login is not working",
        "Please try resetting password",
        "I already tried that",
        "Still not working"
    ]

    esc = engine.save("demo_chat", example_chat)

    print("Conversation escalation score:", round(esc,3))