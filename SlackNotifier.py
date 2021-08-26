import requests
import json

class SlackNotifier:
    def notify_with_message(self, message):
        channel = "cig-info-monitor"
        url = 'https://hooks.slack.com/services/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'
        body = {'text': message, 'channel': channel, 'username':'EOL Hosting Etl - s3 to SQL'}
        requests.post(url, data=json.dumps(body))