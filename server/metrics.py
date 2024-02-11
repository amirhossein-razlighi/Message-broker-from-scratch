import time
import prometheus_client
from prometheus_client.core import CollectorRegistry
from prometheus_client import Summary, Counter, generate_latest, multiprocess, CollectorRegistry

CONTENT_TYPE_LATEST = str('text/plain; version=0.0.3; charset=utf-8')

############ Generate Metrics ############

push_request_metrics = Counter(
    "sad_push_requests", "number of push request")
pull_request_metrics = Counter(
    "sad_pull_requests", "number of requests for pull from this broker")

response_200_metrics = Counter("sad_200_responses", "number of 200 responses of this broker")

resp_insert_request = Summary(
    "sad_insert_response", "response time for push message")


def cal_response_time(start_time, metric_var: prometheus_client.metrics):  # TODO
    """
        Calculates Response time and sends it for metrics route.
    """
    response_time = time.time() - start_time
    return metric_var.observe(response_time)
