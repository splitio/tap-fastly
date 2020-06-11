import os
import json
import asyncio
from pathlib import Path
from itertools import repeat
from urllib.parse import urljoin

import singer
import requests
import pendulum
from singer.bookmarks import write_bookmark, get_bookmark
from pendulum import  period
import datetime
import sys


class FastlyAuthentication(requests.auth.AuthBase):
    def __init__(self, api_token: str):
        self.api_token = api_token

    def __call__(self, req):
        req.headers.update({"Fastly-Key": self.api_token})

        return req


class FastlyClient:
    def __init__(self, auth: FastlyAuthentication, url="https://api.fastly.com"):
        self._base_url = url
        self._auth = auth
        self._session = None

    @property
    def session(self):
        if not self._session:
            self._session = requests.Session()
            self._session.auth = self._auth
            self._session.headers.update({"Accept": "application/json"})

        return self._session

    def _get(self, path, params=None):
        url = urljoin(self._base_url, path)
        response = self.session.get(url, params=params)
        response.raise_for_status()

        return response.json()

    def bill(self, at: datetime):
        try:
            return self._get(f"billing/v2/year/{at.year}/month/{at.month}")
        except Exception as err:
            sys.stderr.write('bill api call exception %s', err)
            return None

    def stats(self, start_date, end_date, params=None):
        try:
            if start_date is not None:
                return self._get(f"stats?from={start_date}&to={end_date}")
            else:
                return self._get(f"stats")
        except Exception as err:
            sys.stderr.write('stats api call exception %s', err)
            return None

    def service(self, service_id):
        try:
            return self._get(f"service/{service_id}")
        except:
            return None

class FastlySync:
    def __init__(self, client: FastlyClient, state={}, config={}):
        self._client = client
        self._state = state
        self._config = config

    @property
    def client(self):
        return self._client

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        singer.write_state(value)
        self._state = value

    def sync(self, stream, schema):
        func = getattr(self, f"sync_{stream}")
        return func(schema)

    async def sync_bills(self, schema, period: pendulum.period = None):
        """Output the `bills` in the period."""
        stream = "bills"
        loop = asyncio.get_event_loop()

        if not period:
            # build a default period from the last bookmark
            bookmark = get_bookmark(self.state, stream, "start_time")
            start = pendulum.parse(bookmark)
            end = pendulum.now()
            period = pendulum.period(start, end)

        singer.write_schema(stream, schema, ["invoice_id"])

        for at in period.range("months"):
            result = await loop.run_in_executor(None, self.client.bill, at)
            if result:
                singer.write_record(stream, result)
                try:
                    end = datetime.datetime.strptime(result["end_time"], "%Y-%m-%dT%H:%M:%SZ").isoformat()
                    self.state = write_bookmark(self.state, stream, "start_time", end)
                except:
                    # print("what fails is:" + result['end_time'])
                    sys.stderr.write("what fails is:" + result['end_time']+"\n")

    async def sync_stats(self, schema, period:pendulum.period = None):
        """Output the stats in the period."""
        stream = "stats"
        loop = asyncio.get_event_loop()

        singer.write_schema(stream, schema, ["service_id", "start_time"])
        bookmark = get_bookmark(self.state, stream, "from")
        if bookmark is not None:
            if "UTC" in bookmark:
                bookmark = datetime.datetime.strptime(bookmark, '%Y-%m-%d %H:%M:%S UTC').isoformat()
            start_date = pendulum.parse(bookmark).int_timestamp
        else:
            start_date = pendulum.parse(self._config['start_date']).int_timestamp
        end_date = pendulum.now().int_timestamp
        result = await loop.run_in_executor(None, self.client.stats, start_date, end_date)
        if result:
            for n in result['data']:
                service_result = await loop.run_in_executor(None, self.client.service, n)
                for i in result['data'][n]:
                    i['service_name'] = service_result['name']
                    i['service_versions'] = json.dumps(service_result['versions'])
                    i['service_customer_id'] = service_result['customer_id']
                    i['service_publish_key'] = service_result['publish_key']
                    i['service_comment'] = service_result['comment']
                    i['service_deleted_at'] = service_result['deleted_at']
                    i['service_updated_at'] = service_result['updated_at']
                    i['service_created_at'] = service_result['created_at']
                    singer.write_record(stream, i)
            try:
                end_temp = datetime.datetime.strptime(result['meta']["to"], '%Y-%m-%d %H:%M:%S UTC')
                end = end_temp.isoformat()
                self.state = write_bookmark(self.state, stream, "from", end)
            except:
                # print("what fails is:" + result['meta']["to"])
                sys.stderr.write("what fails is:" + result['meta']["to"]+"\n")


