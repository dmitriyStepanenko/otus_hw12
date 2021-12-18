import logging
import time
from typing import Dict
from multiprocessing import Queue
import threading

import memcache

from utils import apps_group_to_dict


class Writer(threading.Thread):
    def __init__(
            self,
            queue: Queue,
            memc_client: memcache.Client,
            max_retry: int,
            time_retry: int,
            f_stats_error: Dict[str, int],
            f_stats_success: Dict[str, int],
            dry: bool
    ):
        super(Writer, self).__init__()
        self.queue = queue
        self.client = memc_client
        self.max_retry = max_retry
        self.time_retry = time_retry
        self.f_stats_error = f_stats_error
        self.f_stats_success = f_stats_success
        self.dry = dry

    def run(self):
        while True:
            if not self.queue.empty():
                apps_group = self.queue.get()
                self.queue.task_done()
                if isinstance(apps_group, str):
                    if apps_group == 'quit':
                        break

                out_dict = apps_group_to_dict(apps_group)

                failures = []
                for _ in range(self.max_retry):
                    if self.dry:
                        self.write_dry(out_dict)
                    # вроде как try не нужен, поскольку клиент ловит ошибки
                    failures = self.client.set_multi(out_dict)
                    if len(failures) < len(out_dict):
                        break
                    time.sleep(self.time_retry)

                self.f_stats_error[apps_group.f_name] += len(failures)
                self.f_stats_success[apps_group.f_name] += len(out_dict) - len(failures)

    def write_dry(self, out_dict):
        for key, val in out_dict.items():
            logging.info("%s - %s -> %s" % (self.client.servers[0].address, key, str(val).replace("\n", " ")))
