#!/usr/bin/env python
# -*- coding: utf-8 -*-
import gzip
import sys
import glob
import logging
from optparse import OptionParser
import memcache
from multiprocessing import Manager, Queue
from typing import Dict
from collections import defaultdict
from multiprocessing import Pool
from utils import parse_appsinstalled, dot_rename, AppsGroup, prototest
from writer import Writer


NORMAL_ERR_RATE = 0.01
MAX_BUFF_SIZE = 10


def read_file(
        fn: str,
        queue_by_device: Dict[bytes, Queue],
        f_stats_error: Dict[str, int],
        f_stats_success: Dict[str, int],
        max_buff_size: int
):
    buffers = {d: [] for d in queue_by_device.keys()}
    f_stats_error[fn] = 0
    f_stats_success[fn] = 0
    logging.info('Processing %s' % fn)
    fd = gzip.open(fn)
    errors = 0
    for line in fd:
        line = line.strip()
        if not line:
            continue

        appsinstalled = parse_appsinstalled(line)
        if not appsinstalled:
            errors += 1
            continue

        buff = buffers.get(appsinstalled.dev_type)
        if buff is None:
            errors += 1
            logging.error("Unknow device type: %s" % appsinstalled.dev_type)
            continue

        buff.append(appsinstalled)

        if len(buff) == max_buff_size:
            queue = queue_by_device.get(appsinstalled.dev_type)
            put_apps_in_queue(queue, appsinstalled.dev_type, buff, fn)
            buffers[appsinstalled.dev_type] = []

    for device, buff in buffers.items():
        if len(buff) > 0:
            queue = queue_by_device.get(device)
            put_apps_in_queue(queue, device, buff, fn)

    if not f_stats_success[fn]:
        logging.error("All errors. Failed load")
        fd.close()
        dot_rename(fn)
        return

    f_stats_error[fn] += errors

    err_rate = float(errors) / f_stats_success[fn]
    if err_rate < NORMAL_ERR_RATE:
        logging.info("Acceptable error rate (%s). Successful load" % err_rate)
    else:
        logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))

    fd.close()
    dot_rename(fn)


def put_apps_in_queue(queue, dev_type, apps, fn):
    try:
        if queue.full():
            queue.join()
        queue.put(AppsGroup(device=dev_type, apps=apps, f_name=fn))
    except Exception as e:
        logging.exception(f'Adding in queue failed: {e}')


def main(options):
    device_memc = {
        b"idfa": options.idfa,
        b"gaid": options.gaid,
        b"adid": options.adid,
        b"dvid": options.dvid,
    }
    manager = Manager()
    queue_by_device = {d: manager.Queue(maxsize=options.queue_size) for d in device_memc.keys()}

    f_stats_error = defaultdict(int)
    f_stats_success = defaultdict(int)

    w_workers = []
    for device, adr in device_memc.items():
        w_worker = Writer(
            memc_client=memcache.Client([adr], socket_timeout=options.socket_timeout),
            queue=queue_by_device[device],
            f_stats_error=f_stats_error,
            f_stats_success=f_stats_success,
            max_retry=options.max_retry,
            time_retry=options.time_retry,
            dry=options.dry
        )
        w_worker.start()
        w_workers.append(w_worker)

    with Pool(options.workers) as r_pool:
        for fn in glob.iglob(options.pattern):
            r_pool.apply(
                read_file,
                (
                    fn,
                    queue_by_device,
                    f_stats_error,
                    f_stats_success,
                    MAX_BUFF_SIZE,
                )
            )
    r_pool.join()

    for queue in queue_by_device.values():
        queue.put('quit')


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=True)
    op.add_option("--pattern", action="store",
                  default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    op.add_option("--queue_size", action="store", default=10)
    op.add_option("--workers", action="store", default=3)
    op.add_option("--socket_timeout", action="store", default=3)
    op.add_option("--max_retry", action="store", default=3)
    op.add_option("--time_retry", action="store", default=1)
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
