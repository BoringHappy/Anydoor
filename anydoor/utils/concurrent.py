"""
Concurrent execution utilities for parallel task processing.

This module provides utilities for running tasks in parallel using thread pools,
with support for progress tracking, error handling, and resource management.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, AnyStr, Callable, Dict

from loguru import logger


def get_incompleted_future_counts(_futures):
    """
    Count the number of incomplete futures in a dictionary of futures.

    Args:
        _futures: Dictionary mapping futures to their associated tasks

    Returns:
        int: Number of futures that are not yet completed
    """
    return sum(1 for future in _futures if not future.done())


def as_completed_download_futures(
    running_futures,
    remove_at_done=True,
    stop_at_count=None,
    failed_at_exception=True,
):
    """
    Process completed futures from a running futures dictionary.

    Iterates through completed futures, logs their results, and optionally
    removes them from the running futures dictionary. Supports early termination
    when the number of incomplete futures drops below a threshold.

    Args:
        running_futures (dict): Dictionary mapping futures to their task identifiers
        remove_at_done (bool): Whether to remove completed futures from the dictionary
        stop_at_count (int, optional): Stop processing when incomplete count drops below this value
        failed_at_exception (bool): Whether to raise exceptions from failed futures

    Raises:
        Exception: If failed_at_exception is True and a future raises an exception
    """
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
    """
    Execute tasks in parallel using a thread pool executor.

    Processes a dictionary of tasks by submitting them to a thread pool executor
    and managing the execution with automatic cleanup and error handling.

    Args:
        tasks (Dict[AnyStr, Any]): Dictionary of tasks to execute, where keys are
                                  task identifiers and values are task parameters
        func (Callable): Function to execute for each task
        thread (int): Maximum number of worker threads (default: 6)
        executor: Executor class to use (default: ThreadPoolExecutor)
        failed_at_exception (bool): Whether to raise exceptions from failed tasks

    Raises:
        AssertionError: If any task item is not a dictionary
        Exception: If failed_at_exception is True and a task raises an exception

    Note:
        Each task in the tasks dictionary must be a dictionary that will be
        unpacked as keyword arguments to the function.
    """
    with executor(max_workers=thread) as executor:
        running_futures = {}
        for task in tasks:
            assert isinstance(task, dict), "tasks item must be a dict"
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
