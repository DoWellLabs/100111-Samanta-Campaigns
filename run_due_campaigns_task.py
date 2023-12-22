import requests


def request_task_run():
    session = requests.Session()
    response = session.post(
        url="https://samanta100111.pythonanywhere.com/api/v1/campaigns/webhooks/tasks/",
        json={
            "event": "task_due",
            "task_name": "run_due_campaigns",
            "passkey": "1eb$fyirun-gh2j3go1n4u12@i"
        }
    )
    if response.status_code < 500:
        print(response.json())
    response.raise_for_status()


if __name__ == "__main__":
    request_task_run()

# RUN THIS TASK HOURLY
