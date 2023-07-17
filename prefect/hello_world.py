"""
This is a simple example of a Prefect flow that runs a few tasks in parallel.

The prefect flow does the following:
1. Runs 10 instances of the `hello_task` task in parallel
2. Runs the `goodbye_task` task after all 10 instances of `hello_task` have completed
"""
from typing import List
import logging

import prefect
from prefect.futures import PrefectFuture
from prefect.task_runners import ConcurrentTaskRunner
from prefect.logging import get_run_logger

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

@prefect.task
def hello_task(task_id: int) -> None:
    """
    `hello_task` acts as a workhorse function. Tasks will be the building blocks of your Prefect flows.
    
    Note that the function is decorated with a `prefect.task`, which gives a few different ways to execute the task:
    1. This task can be run sequentially by simply calling the function.
    2. This task can be run asynchronously by calling `hello_task.submit(task_id=your_id)`

    A return value can be given to the task, but it is not required. When called asynchronously, the return value
    can be accessed by calling `hello_task.result()`.

    Parameters
    ----------
    task_id : int
        A dummy integer to pass to logging

    Returns
    -------
    None
    """
    get_run_logger().info(f"Hello, world! This is task {task_id}")


def hello_task_futures() -> List[PrefectFuture]:
    """
    Sometimes, there are a large number of similar tasks that need to be run. In this case, we'll make a "task factory"
    to create a list of tasks that can be run in parallel. We'll use this later in our prefect flow to create 10
    instances of the `hello_task` task.

    Returns
    -------
    List[PrefectFuture]
        A list of `PrefectFuture` objects that can be used to track the progress of the tasks
    """
    futures = []
    for i in range(10):
        futures.append(hello_task.submit(task_id=i))
    return futures


@prefect.task
def goodbye_task() -> None:
    get_run_logger().info("Goodbye, world!")


@prefect.flow(name="hello-flow", task_runner=ConcurrentTaskRunner())
def hello_flow() -> None:
    """
    The `hello_flow()` function ties in all of the tasks we've defined into a Prefect flow. We will define how the
    various tasks interact with each other
    """
    hello = hello_task_futures()
    # The wait_for parameter tells the flow to wait for the list given tasks to complete before running the next task.
    goodbye_task.submit(wait_for=hello)


if __name__ == "__main__":
    # Execute the given flow
    hello_flow()
