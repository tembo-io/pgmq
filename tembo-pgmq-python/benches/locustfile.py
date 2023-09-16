import time

from tembo_pgmq_python import PGMQueue

from locust import task, User


class MyClient(PGMQueue):

    def __init__(self, *args, request_event, **kwargs):
        super().__init__(*args, **kwargs)
        self._request_event = request_event

    def __getattribute__(self, item: str):
        if item not in ("send", "archive", "read", "send_batch"):
            return PGMQueue.__getattribute__(self, item)

        func = PGMQueue.__getattribute__(self, item)

        def wrapper(*args, **kwargs):
            request_meta = {
                "request_type": "pgmq",
                "name": func.__name__,
                "start_time": time.time(),
                "response_length": 0,
                # calculating this for an xmlrpc.client response would be too hard
                "response": None,
                "context": {},  # see HttpUser if you actually want to implement contexts
                "exception": None,
            }
            start_perf_counter = time.perf_counter()
            try:
                request_meta["response"] = func(*args, **kwargs)
            except Exception as e:
                request_meta["exception"] = e
            response_time = (time.perf_counter() - start_perf_counter) * 1000
            request_meta["response_time"] = response_time
            # This is what makes the request actually get logged in Locust
            self._request_event.events.request.fire(**request_meta)
            return request_meta["response"]

        return wrapper


class BaseActor(User):
    """
    A minimal Locust user class that provides an XmlRpcClient to its subclasses
    """

    host = ""
    abstract = True  # dont instantiate this as an actual user when running Locust
    client: PGMQueue
    small_data = {"one": 1, "two": 2}

    def __init__(self, environment):
        super().__init__(environment)
        self.environment = environment
        self.client = MyClient(host="0.0.0.0", port="28815", database="pgmq",
                               username="guru", request_event=environment)
        # just to make sure
        self.client.create_queue("locust")
        self.small_batch = [self.small_data for i in range(50)]


class SingleInsert(BaseActor):
    @task
    def only_insert(self):
        self.client.send("locust", self.small_data)


class BatchInsert(BaseActor):
    @task
    def only_insert(self):
        self.client.send_batch("locust", self.small_batch)


class SlowWorker(BaseActor):
    @task
    def get_and_archive(self):
        job = self.client.read("locust")
        if not job:
            return
        time.sleep(20)
        self.client.archive("locust", job.msg_id)
