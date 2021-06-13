# apitest.py

import time
from locust import HttpUser, task, between
from shared.Helpers import get_OBA_routelist

class QuickstartUser(HttpUser):
    wait_time = between(0.5, 10)

    #todo as written this will try all routes all hours for a single day june 12
    @task(3)
    def view_items(self):
        routelist = get_OBA_routelist()
        for hour in range(23):
            for route in routelist:
                self.client.get(f"/api/v2/nyc/buses/2021/6/12/{hour}/{route}", name="/api/v2/nyc/buses")
                time.sleep(1)

    # def on_start(self):
    #     self.client.post("/login", json={"username":"foo", "password":"bar"})


