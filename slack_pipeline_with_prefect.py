"""Pipeline to load slack into bigquery."""

from typing import List

import dlt
import pendulum
from pendulum import datetime
from prefect import flow, task

from slack import slack_source


@task
def load_channels() -> None:
    """Execute a pipeline that will load a list of all the Slack channels in the
    workspace to BigQuery"""
    pipeline = dlt.pipeline(
        pipeline_name="slack", destination="bigquery", dataset_name="slack_dlt"
    )

    source = slack_source(
        page_size=20,
        selected_channels=None,
    ).with_resources("channels")

    load_info = pipeline.run(
        source,
    )
    print(load_info)


@task
def get_resources() -> List[str]:
    """Fetch a list of available dlt resources so we can fetch them one at a time"""
    resource_dict = slack_source(
        page_size=20,
        selected_channels=None,
    ).resources

    # Remove the non-channel resources
    resource_dict.pop("channels")
    resource_dict.pop("access_logs")
    resource_dict.pop("users")

    return resource_dict.keys()


@task
def load_channel_history(channel: str, start_date: datetime) -> None:
    """Execute a pipeline that will load the given Slack channel
    incrementally beginning at the given start date."""

    pipeline = dlt.pipeline(
        pipeline_name="slack", destination="bigquery", dataset_name="slack_dlt"
    )

    source = slack_source(
        page_size=20,
        selected_channels=[channel],
        start_date=start_date,
    ).with_resources(
        channel,
    )

    load_info = pipeline.run(
        source,
    )
    print(load_info)


@task
def get_users() -> None:
    """Execute a pipeline that will load Slack users list."""

    pipeline = dlt.pipeline(
        pipeline_name="slack", destination="bigquery", dataset_name="slack_dlt"
    )

    source = slack_source(
        page_size=20,
    ).with_resources("users")

    load_info = pipeline.run(
        source,
    )
    print(load_info)


@flow
def slack_pipeline(
    channels=None, start_date=pendulum.now().subtract(days=1).date()
) -> None:
    load_channels()

    resources = get_resources()
    for resource in resources:
        if channels is not None and resource not in channels:
            continue

        load_channel_history(resource, start_date=start_date)

    get_users()


if __name__ == "__main__":
    slack_pipeline.serve("slack_pipeline")
