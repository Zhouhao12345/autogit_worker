from multiprocessing import Manager, Pool
import hashlib
import logging
import git

from mq_server import Consumer
from config import *

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

def consumer(queue_map):
    LOGGER.info("Start Consumer Processing")
    consumer = Consumer(
        "amqp://{username}:{passwd}@{host}:{port}{virtualhost}".format(
            username=MQ_USERNAME,
            passwd=MQ_PASSWORD,
            host=MQ_HOST,
            port=MQ_PORT,
            virtualhost=MQ_VIRTUAL_HOST
        ), queue_map, QUEUE_NAME)
    try:
        consumer.run()
    except KeyboardInterrupt as e:
        LOGGER.warning(e)
        consumer.stop()

def worker(cd, q, index):
    LOGGER.info("Start Worker Processing")
    while True:
        with cd:
            if q.empty():
                cd.wait()
            else:
                order = q.get_nowait()
                try:
                    git_detail_info = git_map[index]
                    git_path = git_detail_info.get("git_path")
                    git_remote_name = git_detail_info.get("git_remote_name")
                    git_branch = git_detail_info.get("git_branch")
                    git_url = git_detail_info.get("git_url")

                    LOGGER.info("Start {order} {git_path} {git_remote_name} "
                                "{git_branch} {git_url}".format(
                        git_path=git_path, git_branch=git_branch, order=order,
                        git_url=git_url, git_remote_name=git_remote_name,
                    ))

                    empty_repo = git.Repo.init(git_path)
                    remote_list = empty_repo.remotes
                    origin = remote_list.origin if len(remote_list) > 0 else False
                    if not origin or not origin.exists():
                        origin = empty_repo.create_remote(
                            git_remote_name,
                            git_url
                        )
                        origin.fetch()
                    empty_repo.create_head(git_branch,
                                           getattr(origin.refs, git_branch))
                    getattr(empty_repo.heads, git_branch).set_tracking_branch(
                        getattr(origin.refs, git_branch))
                    getattr(empty_repo.heads, git_branch).checkout()
                    origin.pull()

                    LOGGER.info("End {order} {git_path} {git_remote_name} "
                                "{git_branch} {git_url}".format(
                        git_path=git_path, git_branch=git_branch, order=order,
                        git_url=git_url, git_remote_name=git_remote_name,
                    ))
                except Exception as e:
                    LOGGER.error(e)

if __name__ == "__main__":
    manager = Manager()
    queue_map = {}
    with Pool(len(git_map) + 1) as p:
        for index, git in enumerate(git_map):
            queue = manager.Queue()
            cd = manager.Condition()
            git_url = git.get("git_url", False)
            git_remote_name = git.get("git_remote_name", False)
            git_branch = git.get("git_branch", False)
            if not git_url or not git_remote_name or not git_branch:
                raise Exception("Please make sure git url, path and "
                                "remote name had been typed")

            h = hashlib.md5()
            h.update((git_url + git_remote_name + git_branch).encode(
                encoding="utf-8")
            )
            route_key = h.hexdigest()
            queue_map[route_key] = (queue, cd)
            p.apply_async(worker, (cd,queue, index))

        p.apply_async(consumer, (queue_map,))

        p.close()
        p.join()
