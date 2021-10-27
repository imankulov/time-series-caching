#!/usr/bin/env python
import abc
import json
import logging
import os
import sys
import typing
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Generic, Optional, Type, TypeVar

import pydantic
import redis
from apscheduler.executors.pool import ProcessPoolExecutor
from apscheduler.schedulers.blocking import BlockingScheduler
from dash import Dash, dcc, html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from elasticsearch.client import Elasticsearch
from elasticsearch_dsl import Q, Search
from flask import Flask
from flask_apscheduler import APScheduler
from pydantic import BaseModel, validator
from pydantic.json import pydantic_encoder

# ----------------------------------------------------------------------------------
# Global constants
# ----------------------------------------------------------------------------------
logger = logging.getLogger(__name__)
week = timedelta(days=7)

# ----------------------------------------------------------------------------------
# ElasticSearch (upstream)
# ----------------------------------------------------------------------------------
# Create your free account at https://www.anyblockanalytics.com/.
# Locally, set the following environment variables.
#
# ES_USERNAME to your email from
# https://account.anyblock.tools/user/profile/
#
# ES_PASSWORD to your API key from
# https://account.anyblock.tools/user/apitokens/
#
# See https://apidocs.anyblock.tools/#/Elastic to get more info about the
# ElasticSearch API endpoints.
es_username = os.environ["ES_USERNAME"]
es_password = os.environ["ES_PASSWORD"]
es_url = (
    f"https://{es_username}:{es_password}@"
    f"api.anyblock.tools/bitcoin/bitcoin/mainnet/es/"
)
es_client = Search(using=Elasticsearch(hosts=[es_url]))


# ----------------------------------------------------------------------------------
# Redis (cache)
# ----------------------------------------------------------------------------------
# Run a local Redis installation with "docker-compose up"
redis_client = redis.Redis.from_url(os.environ["REDIS_URL"])


# ----------------------------------------------------------------------------------
# Scheduler (cron)
# ----------------------------------------------------------------------------------
scheduler_executors = {
    "default": ProcessPoolExecutor(16),
}
scheduler = APScheduler(scheduler=BlockingScheduler(executors=scheduler_executors))


# ----------------------------------------------------------------------------------
# Flask, Dash and services initialization
# ----------------------------------------------------------------------------------
# The dash app requires a layout attribute. It's defined below with the layout()
# function.
flask = Flask("app")
app = Dash(server=flask)
scheduler.init_app(flask)
logging.basicConfig(level=logging.DEBUG)

# ----------------------------------------------------------------------------------
# Models
# ----------------------------------------------------------------------------------


class TimestampModel(BaseModel):
    """Base model for any records having timestamps."""

    timestamp: datetime


class Transaction(TimestampModel):
    """Model to keep track of the individual transaction."""

    amount: Decimal
    transaction_hash: str

    class Config:
        # Unless strongly necessary otherwise, we prefer define our models as frozen.
        frozen = True


class WeeklyBucket(BaseModel):
    """Model to represent a weekly bucket, the unit of fetching data."""

    start: datetime

    @property
    def end(self) -> datetime:
        return self.start + week

    @validator("start")
    def align_start(cls, v: datetime) -> datetime:
        """Align weekly bucket start date."""
        seconds_in_week = week.total_seconds()
        return datetime.fromtimestamp(
            (v.timestamp() // seconds_in_week * seconds_in_week), timezone.utc
        )

    def next(self) -> "WeeklyBucket":
        """Return the next bucket."""
        return WeeklyBucket(start=self.end)

    def cache_key(self) -> str:
        """Helper function to return the cache key by the bucket start date."""
        return f"{int(self.start.timestamp())}"

    class Config:
        frozen = True


def get_buckets(start_date: datetime, end_date: datetime) -> list[WeeklyBucket]:
    """Return the list of weekly buckets in a date range."""
    buckets: list[WeeklyBucket] = []

    if end_date < start_date:
        raise ValueError(f"{end_date=} must be greater than {start_date=}")

    bucket = WeeklyBucket(start=start_date)
    while True:
        buckets.append(bucket)
        bucket = bucket.next()
        if bucket.end >= end_date:
            break
    return buckets


# ----------------------------------------------------------------------------------
# Cache utils
# ----------------------------------------------------------------------------------

T = TypeVar("T", bound=TimestampModel)


class GenericFetcher(abc.ABC, Generic[T]):
    """Generic cache fetcher.

    Notice the following things.

    - We define the GenericFetcher as an abstract base class (inherited from ABC)
      and defined some methods as abstract. It's an extra saftey net. The interpreter
      prohibits instantiation of abstract classes or their subclasses without all
      abstract methods defined. Read more about abstract base
      classes at https://docs.python.org/3/library/abc.html

    - On top of this, we define this class as "generic." In our case, it means that
      subclasses can specify with types methods  fetch() and get_values_from_upstream()
      will return. Read more: https://mypy.readthedocs.io/en/stable/generics.html
    """

    @abc.abstractmethod
    def get_cache_key(self, bucket: WeeklyBucket) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_values_from_upstream(self, bucket: WeeklyBucket) -> list[T]:
        raise NotImplementedError()

    def fetch(self, start_date: datetime, end_date: datetime) -> list[T]:
        """The main class entrypoint.

        Fetch and return the list of records between start_date (inclusive) and
        end_date (exclusive).
        """
        buckets = get_buckets(start_date, end_date)
        model_type = self.get_model_type()
        records: list[T] = []
        for bucket in buckets:

            # Check if there's anything in cache
            cache_key = self.get_cache_key(bucket)
            cached_raw_value = redis_client.get(cache_key)
            if cached_raw_value is not None:
                records += pydantic.parse_raw_as(
                    list[model_type], cached_raw_value  # type: ignore
                )
                continue

            # Fetch the value from the upstream
            value = self.get_values_from_upstream(bucket)

            # Save the value to the cache
            raw_value = json.dumps(
                value, separators=(",", ":"), default=pydantic_encoder
            )
            redis_client.set(cache_key, raw_value, ex=get_cache_ttl(bucket))

            records += value

        return [
            record for record in records if start_date <= record.timestamp < end_date
        ]

    def get_model_type(self) -> Type:
        """Python non-documented black magic to fetch current model.

        Ref: https://stackoverflow.com/a/60984681
        """
        parametrized_generic_fetcher = self.__orig_bases__[0]  # type: ignore
        return typing.get_args(parametrized_generic_fetcher)[0]


def get_cache_ttl(bucket: WeeklyBucket):
    if bucket.end >= datetime.now(tz=timezone.utc):
        return int(timedelta(minutes=10).total_seconds())
    return int(timedelta(days=30).total_seconds())


# ----------------------------------------------------------------------------------
# Transaction Fetcher
# ----------------------------------------------------------------------------------


class TransactionFetcher(GenericFetcher[Transaction]):
    """Transaction fetcher.

    Usage example:

        fetcher = TransactionFetcher("1HesYJSP1QqcyPEjnQ9vzBL1wujruNGe7R")
        transactions = fetcher.fetch(start_date, end_date)
    """

    def __init__(self, wallet_address: str):
        self.wallet_address = wallet_address

    def get_cache_key(self, bucket: WeeklyBucket) -> str:
        return f"transactions:{self.wallet_address}:{bucket.cache_key()}"

    def get_values_from_upstream(self, bucket: WeeklyBucket) -> list[Transaction]:
        return fetch_transactions(self.wallet_address, bucket.start, bucket.end)


# ----------------------------------------------------------------------------------
# Query
# ----------------------------------------------------------------------------------


def fetch_transactions(
    wallet_address: str, start_date: datetime, end_date: datetime
) -> list[Transaction]:
    """Low-level query to fetch BTC transactions from ElasticSearch.

    Notice how it doesn't know anything about caching or date buckets. Its job is to
    pull some data from ElasticSearch, convert them to model instances, and return
    them as a list.
    """
    logger.info(
        f"Fetch transactions for {wallet_address=} "
        f"({start_date=}, {end_date=}) from the upstream"
    )

    search = (
        es_client.index("output")
        .filter(
            "range",
            timestamp={
                "gte": int(start_date.timestamp()),
                "lt": int(end_date.timestamp()),
            },
        )
        .filter(
            "nested",
            path="scriptPubKey",
            query=Q(
                "bool", filter=[Q("terms", scriptPubKey__addresses=[wallet_address])]
            ),
        )
    )

    # We use scan() instead of execute() to fetch all the records, and wrap it with a
    # list() to pull everything in memory. As long as we fetch data in buckets of
    # limited sizes, memory is not a problem.
    result = list(search.scan())
    return [
        Transaction(
            timestamp=datetime.fromtimestamp(record.timestamp).replace(
                tzinfo=timezone.utc
            ),
            amount=record["value"]["raw"],
            transaction_hash=record["transactionHash"],
        )
        for record in result
    ]


# ----------------------------------------------------------------------------------
# App layout and callbacks
# ----------------------------------------------------------------------------------


def layout():
    """Display the app layout."""
    # Most of the code is inline CSS to display three rows of the layout:
    # title, controls and plot.
    title_row = html.Div(
        children=[
            html.A(
                href="https://bitcoinforcharity.com/bitcoin-charity-list/",
                target="_blank",
                children="Try some addresses from the Bitcoin Charity List",
            )
        ]
    )

    control_row = html.Div(
        style={
            "font-size": "14px",
            "font-family": "Arial Helvetica sans-serif",
            "display": "flex",
            "gap": "30px",
        },
        children=[
            dcc.Input(
                id="wallet_address",
                style={"padding": "1em", "flex-grow": "8"},
                value="1HesYJSP1QqcyPEjnQ9vzBL1wujruNGe7R",
            ),
            dcc.DatePickerRange(
                id="range",
                style={"flex-grow": "1"},
                start_date="2021-09-01",
                end_date="2021-10-01",
            ),
            html.Button(
                id="button",
                children="Update",
                style={"padding": "1em", "flex-grow": "1"},
            ),
        ],
    )
    plot_row = html.Div(dcc.Loading(id="plot"))

    return html.Div(
        style={
            "display": "flex",
            "flex-direction": "column",
            "padding": "50px",
            "gap": "30px",
        },
        children=[title_row, control_row, plot_row],
    )


@app.callback(
    Output("plot", "children"),
    Input("button", "n_clicks"),
    [
        State("wallet_address", "value"),
        State("range", "start_date"),
        State("range", "end_date"),
    ],
)
def create_plot(
    n_clicks, wallet_address, start_date_str: Optional[str], end_date_str: Optional[str]
):
    if not (wallet_address and start_date_str and end_date_str):
        raise PreventUpdate()

    def to_datetime(value):
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    start_date = to_datetime(start_date_str)
    end_date = to_datetime(end_date_str)

    transactions = TransactionFetcher(wallet_address).fetch(start_date, end_date)
    return dcc.Graph(
        figure={
            "data": [
                {
                    "x": [tx.timestamp for tx in transactions],
                    "y": [tx.amount for tx in transactions],
                    "hovertext": [tx.transaction_hash for tx in transactions],
                    "type": "bar",
                    "name": "BTC Transactions",
                },
            ],
            "layout": {
                "title": f"BTC Transactions to {wallet_address}",
                "xaxis": {"range": [start_date, end_date]},
            },
        },
    )


app.layout = layout()


# ----------------------------------------------------------------------------------
# Cache warmup
# ----------------------------------------------------------------------------------


WALLET_ADDRESSES = [
    # Samples taken from https://bitcoinforcharity.com/bitcoin-charity-list/
    "1HesYJSP1QqcyPEjnQ9vzBL1wujruNGe7R",
    "16Sy8mvjyNgCRYS14m1Rtca3UfrFPzz9eJ",
    "1M72Sfpbz1BPpXFHz9m3CdqATR44Jvaydd",
]


@scheduler.task(
    id="warmup_transaction_cache",
    trigger="interval",
    seconds=1000,
    jitter=100,
    start_date=datetime.now(tz=timezone.utc) + timedelta(seconds=5),
)
def warmup_transaction_cache():
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=30)
    for wallet_address in WALLET_ADDRESSES:
        TransactionFetcher(wallet_address).fetch(start_date, end_date)


if __name__ == "__main__":
    runners = {
        "server": lambda: app.run_server(debug=True),
        "cron": lambda: scheduler.scheduler.start(),
    }
    try:
        runner_name = sys.argv[1]
    except IndexError:
        runner_name = "server"
    runners[runner_name]()
