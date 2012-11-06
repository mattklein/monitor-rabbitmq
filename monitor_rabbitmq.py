from datetime import datetime
import json
import time

import httplib2

from instance_mgmt import RABBITMQ_ADMIN_USERNAME, RABBITMQ_ADMIN_PW, RABBITMQ_API_QUEUE_URL

RABBITMQ_MONITORING_CACHE_FILENAME = '/mnt/ebs/rabbitmq_monitoring_cache.json'


def _get_queue_info_from_rabbitmq():
    """Obtains the queue info from the RabbitMQ API.  Returns a {queue_name: queue_length} dict."""
    h = httplib2.Http()
    h.add_credentials(RABBITMQ_ADMIN_USERNAME, RABBITMQ_ADMIN_PW)
    resp, content = h.request(RABBITMQ_API_QUEUE_URL('db-primy', '55672'))
    assert resp.status == 200
    queues = json.loads(content)  # A list of dicts
    queue_info = {}
    for q in queues:
        queue_info[q['name']] = q['backing_queue_status']['len']
    return queue_info


def _get_queue_info_from_cache():
    """Returns the {queue_name: queue_info} dict that's stored in the cache."""
    try:
        with open(RABBITMQ_MONITORING_CACHE_FILENAME, 'rb') as cache_file:
            return json.load(cache_file)
    except:
        return {}


def _put_queue_info_into_cache(queue_info):
    """Takes the queue_info dict and stores it in the cache.  The cache is a JSON
    file on disk."""
    with open(RABBITMQ_MONITORING_CACHE_FILENAME, 'wb') as cache_file:
        json.dump(queue_info, cache_file)

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'


def get_rabbitmq_queue_info():
    cur_datetime = datetime.utcnow()
    cur_secs_since_epoch = time.mktime(cur_datetime.timetuple())
    cur_queue_info = _get_queue_info_from_rabbitmq()
    cached_queue_info = _get_queue_info_from_cache()
    new_queue_info = {}
    for queue_name, queue_length in cur_queue_info.items():
        if queue_length == 0:
            date_at_last_empty_queue = cur_datetime
        else:
            cached_info = cached_queue_info.get(queue_name)
            if cached_info:
                date_at_last_empty_queue = datetime.strptime(cached_info['date_at_last_empty_queue'], DATETIME_FORMAT)
            else:
                # There's no cached entry for this queue (like because it's a newly-configured queue)
                # Even though its length isn't 0, we'll treat it as if it were, so that we can establish
                #    a baseline date for alerting (i.e., so that we'll be alerted if X minutes pass until
                #    the queue is empty)
                date_at_last_empty_queue = cur_datetime

        secs_since_last_empty_queue = (cur_datetime - date_at_last_empty_queue).total_seconds()

        new_queue_info[queue_name] = {'date': cur_datetime.strftime(DATETIME_FORMAT),
                                      'date_as_secs_since_epoch': cur_secs_since_epoch,
                                      'len': queue_length,
                                      'date_at_last_empty_queue': date_at_last_empty_queue.strftime(DATETIME_FORMAT),
                                      'secs_since_last_empty_queue': secs_since_last_empty_queue}

    # Now we've got a newly constructed queue_info dict for the cache
    _put_queue_info_into_cache(new_queue_info)
    return new_queue_info
