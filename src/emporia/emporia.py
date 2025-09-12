import requests
import time
from datetime import datetime, timezone, timedelta
from emporia.cognito_auth import CognitoAuth
from emporia.enums import Scale, Unit


USER_POOL_ID = 'us-east-2_ghlOXVLi1'
REGION = 'us-east-2'
API_ROOT = 'https://api.emporiaenergy.com'
API_CUSTOMER_DEVICES = "customers/devices"
API_DEVICES_USAGE = "AppAPI?apiMethod=getDeviceListUsages&deviceGids={deviceGids}&instant={instant}&scale={scale}&energyUnit={unit}"
API_CHART_USAGE = "AppAPI?apiMethod=getChartUsage&deviceGid={deviceGid}&channel={channel}&start={start}&end={end}&scale={scale}&energyUnit={unit}"

class Emporia(object):
    def __init__(self, username, password, client_id):
        self._username = username
        self._password = password
        self._pool_wellknown_jwks = None
        self._max_retry_attempts = 5
        self._initial_retry_delay = 0.5
        self._max_retry_delay = 30.0
        self._tokens = {}
        self._customer = {}
        self._connect_timeout = 6.03
        self._read_timeout = 10.03
        self._channels = {}
        self._gids = {}
        self._cognito = CognitoAuth(client_id, USER_POOL_ID, REGION)
        self._cognito.login(username, password)

    def get_devices(self):
        response = self._request(API_CUSTOMER_DEVICES)
        response.raise_for_status()
        devices = []
        if not response.text:
            return devices
        data = response.json()
        if "devices" not in data:
            return devices
        for dev in data["devices"]:
            if 'locationProperties' in dev and 'displayName' in dev['locationProperties']:
                name = dev['locationProperties']['displayName']
            else:
                name = 'Unknown'
            self._gids[dev['deviceGid']] = name
            sub_devices = dev.get("devices", [])
            for sub_dev in sub_devices:
                channels = sub_dev.get("channels")
                if not channels:
                    print(f'sub_dev: {sub_dev}')
                    continue
                for channel in channels:
                    channel_id = f"{channel['deviceGid']}_{channel['channelNum']}"
                    self._channels[channel_id] = channel
        return devices

    def get_usage(self, scale:Scale = Scale.DAY, unit:Unit = Unit.KWH, days_back:int = 0):
        if len(self._gids) == 0:
            self.get_devices()
        gids = "+".join(map(str, self._gids))
        instant = ((datetime.now(timezone.utc) - timedelta(days=days_back))
                   .replace(hour=23, minute=59, second=59, microsecond=999))
        path = API_DEVICES_USAGE.format(
            deviceGids=gids, instant=_format_time(instant), scale=scale.value, unit=unit.value
        )
        response = self._request(path)
        response.raise_for_status()
        usages = self._load_usage(instant, scale.value, unit.value, response.json())
        return usages

    def _load_usage(self, instant, scale, unit, usage):
        usages = []
        if 'deviceListUsages' in usage and 'devices' in usage['deviceListUsages']:
            for device in usage['deviceListUsages']['devices']:
                if 'channelUsages' in device:
                    for usage in device['channelUsages']:
                        if usage['name'] == 'Main':
                            usage['name'] = self._gids[usage['deviceGid']]
                        usages.append(usage)
                        if 'nestedDevices' in usage:
                            for nested_device in usage['nestedDevices']:
                                if 'channelUsages' in nested_device:
                                    for nested_usage in nested_device['channelUsages']:
                                        if nested_usage['name'] == 'Main':
                                            nested_usage['name'] = self._gids[nested_usage['deviceGid']]
                                        usages.append(nested_usage)
                else:
                    print('no channelUsages')

        for usage in usages:
            usage['instant'] = instant
            usage['scale'] = scale
            usage['unit'] = unit
            if 'nestedDevices' in usage:
                usage.pop('nestedDevices')
        return usages

    def get_chart_usage(self, name:str = 'Pond', start: datetime = None, end: datetime = None):
        scale = Scale.DAY.value
        unit = Unit.USD.value
        if not start:
            start = datetime.now(timezone.utc) - timedelta(days=30)
        if not end:
            end = datetime.now(timezone.utc)
        if len(self._channels) == 0:
            self.get_devices()

        for key, channel in self._channels.items():
            if channel['name'] == name:
                path = API_CHART_USAGE.format(
                    deviceGid=channel['deviceGid'],
                    channel=channel['channelNum'],
                    start=_format_time(start),
                    end=_format_time(end),
                    scale=scale,
                    unit=unit,
                )
                response = self._request(path)
                response.raise_for_status()
                return response.json()
        return None

    def _request(self, path: str, method: str = 'get', **kwargs) -> requests.Response:
        response = None
        attempts = 0
        while attempts < self._max_retry_attempts:
            attempts += 1
            response = self._make_request(path, method, **kwargs)
            if response.status_code == 401:
                # if unauthorized, try refreshing the tokens
                self._cognito.refresh_tokens()
                response = self._make_request(path, method, **kwargs)
            if response.status_code >= 500:
                # if server error, retry with exponential backoff
                delay = min(self._initial_retry_delay * (2 ** (attempts - 1)), self._max_retry_delay)
                time.sleep(delay)
                continue
            if response.status_code < 500:
                return response
        return response

    def _make_request(self, path: str, method: str, **kwargs) -> requests.Response:
        headers = kwargs.get("headers")
        if headers is None:
            headers = {}
        else:
            headers = dict(headers)
        headers["authtoken"] = self._cognito.get_id_token()
        url = f"{API_ROOT}/{path}"
        return requests.request(
            method,
            url,
            **kwargs,
            headers=headers,
            timeout=(self._connect_timeout, self._read_timeout),
        )


def _format_time(input_time: datetime) -> str:
    """Convert time to utc, then format"""
    # check if aware
    if input_time.tzinfo and input_time.tzinfo.utcoffset(input_time) is not None:
        # aware, convert to utc
        input_time = input_time.astimezone(timezone.utc)
    else:
        # unaware, assume it's already utc
        input_time = input_time.replace(tzinfo=timezone.utc)
    input_time = input_time.replace(tzinfo=None)  # make it unaware
    return input_time.isoformat() + "Z"
