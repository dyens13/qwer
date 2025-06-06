import requests

# to find your id, find 'userinfobot' on telegram
user = 'your_id'
# to get your bot token, find 'BotFather' on telegram and follow instruction
your_bot = 'your_token'


def send_telegram(message, token=your_bot, to_id=user):
    url = f'https://api.telegram.org/bot{token}/sendMessage'
    try:
        resp = requests.post(url, data={'chat_id': to_id, 'text': message})
        resp.raise_for_status()

        result = resp.json()
        if not result.get('ok', False):
            raise RuntimeError(f"Telegram API error: {result}")

    except requests.exceptions.RequestException as e:
        print(f"[send_telegram] Request exception occurred: {e}")
        raise
    except ValueError:
        print("[send_telegram] Could not parse Telegram response as JSON.")
        raise
    except Exception as e:
        print(f"[send_telegram] Telegram API returned an error: {e}")
        raise
    return True


if __name__ == '__main__':
    send_telegram('test')
