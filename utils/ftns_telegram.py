import telegram
# pip install python-telegram-bot==13.15
# pip install python-telegram-bot --upgrade

# to find your id, find 'userinfobot' on telegram
user = 'your_id'

# to get your bot token, find 'BotFather' on telegram and follow instruction
your_bot = telegram.Bot(token='your_token')


def send_telegram(text, bot=your_bot, to_id=user):
    if isinstance(bot, str):
        bot = globals()[bot]
    try:
        bot.sendMessage(chat_id=to_id, text=text)
    except Exception as e:
        print(e)
        pass
    return None


if __name__ == '__main__':
    send_telegram('test')
