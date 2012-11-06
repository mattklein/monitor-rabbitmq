import collectd

from monitoring import monitor_rabbitmq


def read(data=None):
    vl = collectd.Values(type='gauge')
    queue_info_dicts = monitor_rabbitmq.get_rabbitmq_queue_info()
    for queue_name, queue_info in queue_info_dicts.items():
        vl.plugin = 'rabbitmq-%s-mins-since-empty' % queue_name
        vl.dispatch(values=[queue_info['secs_since_last_empty_queue'] / 60.])


def write(vl, data=None):
    for val in vl.values:
        print "%s (%s): %f" % (vl.plugin, vl.type, val)

collectd.register_read(read)
collectd.register_write(write)
