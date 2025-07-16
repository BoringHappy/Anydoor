from concurrent.futures import ThreadPoolExecutor, as_completed

from loguru import logger


def get_incompleted_future_counts(_futures):
    return sum(1 for future in _futures if not future.done())


def as_completed_download_futures(
    download_futures,
    remove_at_done=True,
    stop_at_count=None,
    failed_at_exception=True,
):
    for future in as_completed(download_futures):
        done_key = download_futures[future]
        incomplete_count = get_incompleted_future_counts(download_futures)
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
            del download_futures[future]
        if stop_at_count:
            if incomplete_count < stop_at_count:
                break


def run_parallel(tasks, func, thread=6, executor=ThreadPoolExecutor):
    with executor(max_workers=thread) as executor:
        download_futures = {}
        for task in tasks:
            download_futures[executor.submit(func, *task)] = task

            if len(download_futures) > 2 * thread:
                as_completed_download_futures(
                    download_futures,
                    remove_at_done=True,
                    stop_at_count=thread,
                )

        as_completed_download_futures(download_futures)
