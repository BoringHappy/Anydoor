from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, AnyStr, Callable, Dict

from loguru import logger


def get_incompleted_future_counts(_futures):
    return sum(1 for future in _futures if not future.done())


def as_completed_download_futures(
    running_futures,
    remove_at_done=True,
    stop_at_count=None,
    failed_at_exception=True,
):
    for future in as_completed(running_futures):
        done_key = running_futures[future]
        incomplete_count = get_incompleted_future_counts(running_futures)
        try:
            return_result = future.result()
            logger.info(
                f"Finish Load {done_key}, Rest:{incomplete_count} Result: {return_result}"
            )
        except Exception as e:
            logger.warning(f"Failed to load {done_key}, Error: {e}")
            if failed_at_exception:
                raise e
        if remove_at_done:
            del running_futures[future]
        if stop_at_count and (incomplete_count < stop_at_count):
            return


def run_parallel(
    tasks: Dict[AnyStr, Any],
    func: Callable,
    thread: int = 6,
    executor=ThreadPoolExecutor,
    failed_at_exception: bool = True,
):
    with executor(max_workers=thread) as executor:
        running_futures = {}
        for task in tasks:
            running_futures[executor.submit(func, **task)] = task

            if len(running_futures) > 2 * thread:
                as_completed_download_futures(
                    running_futures=running_futures,
                    remove_at_done=True,
                    stop_at_count=thread,
                    failed_at_exception=failed_at_exception,
                )

        as_completed_download_futures(
            running_futures=running_futures, failed_at_exception=failed_at_exception
        )
