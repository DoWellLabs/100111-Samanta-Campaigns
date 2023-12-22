import requests
import time


def request_task_run():
    session = requests.Session()
    response = session.post(
        url="https://samanta100111.pythonanywhere.com/api/v1/campaigns/webhooks/tasks/",
        json={
            "event": "task_due",
            "task_name": "crawl_campaigns_leads_links",
            "passkey": "1eb$fyirun-gh2j3go1n4u12@i"
        }
    )
    if response.status_code < 500:
        print(response.json())
    response.raise_for_status()


if __name__ == "__main__":
    while True:
        request_task_run()
        time.sleep(60 * 5)


# RUN THIS TASK EVERY 5 MINUTES (USE AN ALWAYS-ON TASK ON PYTHONANYWHERE)
