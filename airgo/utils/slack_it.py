import os
import json
import requests


def slack_it(
    text,
    username="airgo-bot",
    channel=None,
    slack_http_endpoint=os.getenv("SLACK_HTTPS_ENDPOINT"),
    color=None,
    icon_emoji=":ghost:",
    **kwargs
):
    channel = (
        channel
        if channel is not None
        else (
            "#airgo-test-messages"
            if os.getenv("CONNECTION_ENV", "compose") in ["laptop", "compose"]
            else "#airgo-messages"
        )
    )
    url = "https://{}".format(slack_http_endpoint)
    payload = {
        "channel": channel,
        "username": username,
        "text": text,
        "icon_emoji": icon_emoji,
        "parse": "full",
    }
    if color is not None:
        payload["color"] = color
    payload.update(**kwargs)
    requests.post(url, data=json.dumps(payload))
