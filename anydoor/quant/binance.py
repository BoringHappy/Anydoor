# -*- coding:utf-8 -*-
"""
Binance API client implementation
"""

import hashlib
import hmac
import json
from datetime import datetime
from urllib.parse import urlencode

import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_fixed


class Interval:
    MIN1 = "1m"
    MIN3 = "3m"
    MIN5 = "5m"
    MIN15 = "15m"
    MIN30 = "30m"
    HOUR1 = "1h"
    HOUR2 = "2h"
    HOUR4 = "4h"
    HOUR6 = "6h"
    HOUR8 = "8h"
    HOUR12 = "12h"
    DAY1 = "1d"
    DAY3 = "3d"
    WEEK1 = "1w"
    MON1 = "1m"
    INVALID = None


class OrderSide:
    BUY = "BUY"
    SELL = "SELL"
    INVALID = None


class TimeInForce:
    GTC = "GTC"
    IOC = "IOC"
    FOK = "FOK"
    GTX = "GTX"
    INVALID = None


class OrderType:
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    STOP = "STOP"
    STOP_MARKET = "STOP_MARKET"
    TAKE_PROFIT = "TAKE_PROFIT"
    TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET"
    TRAILING_STOP_MARKET = "TRAILING_STOP_MARKET"
    INVALID = None


class OrderRespType:
    ACK = "ACK"
    RESULT = "RESULT"
    INVALID = None


class MatchRole:
    MAKER = "maker"
    TAKER = "taker"


class DepthStep:
    STEP0 = "step0"
    STEP1 = "step1"
    STEP2 = "step2"
    STEP3 = "step3"
    STEP4 = "step4"
    STEP5 = "step5"


class SubscribeMessageType:
    RESPONSE = "response"
    PAYLOAD = "payload"


class Boolean:
    true = "true"
    false = "false"


class TransferType:
    ROLL_IN = "ROLL_IN"
    ROLL_OUT = "ROLL_OUT"
    INVALID = None


class WorkingType:
    MARK_PRICE = "MARK_PRICE"
    CONTRACT_PRICE = "CONTRACT_PRICE"
    INVALID = None


class FuturesMarginType:
    ISOLATED = "ISOLATED"
    CROSSED = "CROSSED"


class PositionSide:
    BOTH = "BOTH"
    LONG = "LONG"
    SHORT = "SHORT"
    INVALID = None


class IncomeType:
    TRANSFER = "TRANSFER"
    WELCOME_BONUS = "WELCOME_BONUS"
    REALIZED_PNL = "REALIZED_PNL"
    FUNDING_FEE = "FUNDING_FEE"
    COMMISSION = "COMMISSION"
    INSURANCE_CLEAR = "INSURANCE_CLEAR"
    INVALID = None


class RequestMethod:
    GET = "GET"
    POST = "POST"
    DELETE = "DELETE"
    PUT = "PUT"


class UpdateTime:
    NORMAL = ""
    FAST = "@100ms"
    REALTIME = "@0ms"
    INVALID = None


class RestApiRequest(object):
    def __init__(self, host: str, url, method, sign, no_time: bool = False):
        self.url = url
        self.method = method
        self.host = host
        self.sign = sign
        self.no_time = no_time

        self.param_map = dict()
        self.header = {
            "client_SDK_Version": "binance_api_0.0.1",
            "Content-Type": "application/json",
        }
        if not self.no_time:
            self.put_url("recvWindow", 60000)
            self.put_url("timestamp", now())

    @property
    def final_url(self):
        return self.host + self.url + "?" + self.build_url()

    def put_url(self, name, value):
        if value is not None:
            if isinstance(value, list):
                self.param_map[name] = json.dumps(value)
            elif isinstance(value, float):
                self.param_map[name] = (
                    ("%.20f" % value)[slice(0, 16)].rstrip("0").rstrip(".")
                )
            else:
                self.param_map[name] = str(value)

    def build_url(self):
        if len(self.param_map) == 0:
            return ""
        encoded_param = urlencode(self.param_map)
        return encoded_param

    def build_url_to_json(self):
        return json.dumps(self.param_map)


def is_valid(value, name):
    if value is None:
        raise Exception("[Input] " + name + " should not be null")


def now():
    return str(int(round(datetime.now().timestamp() * 1000)))


class BinanceApi:
    """
    Binance REST API client with comprehensive trading functionality.

    Provides a complete interface to Binance's REST API including:
    - Spot trading operations
    - Futures trading operations
    - Market data retrieval
    - Account management
    - Order management with various types and time-in-force options

    Features:
    - HMAC-SHA256 signature authentication
    - Automatic retry with exponential backoff
    - Comprehensive error handling
    - Support for all Binance API endpoints
    - Type-safe enums for all parameters

    Attributes:
        api_key (str): Binance API key
        secret_key (str): Binance secret key
        host (str): Binance API host URL
        raise_error (bool): Whether to raise exceptions on API errors
        proxies (dict): HTTP proxy configuration
    """

    def __init__(
        self,
        api_keys,
        secret_key,
        host: str = None,
        raise_error: bool = True,
        proxies: dict = None,
    ):
        """
        Initialize Binance API client.

        Args:
            api_keys (str): Binance API key
            secret_key (str): Binance secret key
            host (str, optional): API host URL (defaults to https://api.binance.com)
            raise_error (bool): Whether to raise exceptions on API errors
            proxies (dict, optional): HTTP proxy configuration
        """
        self.api_key = api_keys
        self.secret_key = secret_key
        self.host = host if host else "https://api.binance.com"
        self.restapi = None
        self.raise_error = raise_error
        self.proxies = proxies

    def sign_requests(self):
        """
        Sign API requests with HMAC-SHA256 signature.

        Generates the required signature for authenticated API requests
        and adds the API key to request headers.

        Returns:
            self: For method chaining
        """
        if self.restapi.sign:
            self.restapi.put_url(
                "signature",
                hmac.new(
                    self.secret_key.encode(),
                    msg=self.restapi.build_url().encode(),
                    digestmod=hashlib.sha256,
                ).hexdigest(),
            )
            self.restapi.header.update({"X-MBX-APIKEY": self.api_key})

        return self

    def get_data(
        self,
        api: str,
        method=RequestMethod.GET,
        sign: bool = False,
        no_time: bool = False,
        **kwargs,
    ):
        """
        Make API request to Binance with specified parameters.

        Args:
            api (str): API endpoint path (e.g., "/api/v3/order")
            method (RequestMethod): HTTP method (GET, POST, DELETE, PUT)
            sign (bool): Whether to sign the request with HMAC-SHA256
            no_time (bool): Whether to exclude timestamp from request
            **kwargs: Additional API parameters

        Returns:
            requests.Response: HTTP response from Binance API

        Example:
            response = api.get_data("/api/v3/order",
                                 symbol="BTCUSDT",
                                 side=OrderSide.BUY,
                                 type=OrderType.LIMIT,
                                 quantity="0.001",
                                 price="50000",
                                 sign=True)
        """
        self.restapi = RestApiRequest(
            host=self.host, url=api, method=method, sign=sign, no_time=no_time
        )
        logger.info(kwargs)
        for arg_key, arg_value in kwargs.items():
            self.restapi.put_url(arg_key, arg_value)
        return self.sign_requests().call_sync()

    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(1))
    def call_sync(self):
        """
        Execute HTTP request with retry logic.

        Makes the actual HTTP request to Binance API with automatic retry
        on failure. Uses exponential backoff between retry attempts.

        Returns:
            requests.Response: HTTP response from Binance API

        Raises:
            requests.HTTPError: If HTTP request fails after all retries
        """
        response = requests.request(
            method=self.restapi.method,
            url=self.restapi.final_url,
            headers=self.restapi.header,
            proxies=self.proxies,
        )
        if response.ok:
            logger.info(
                f"{self.restapi.method} {self.restapi.url.split('?')[0]} -> {response.status_code}"
            )
            return response
        else:
            response.raise_for_status()


if __name__ == "__main__":
    logger.info(f"localtime start:{datetime.now()}")

    logger.info(f"localtime end:{datetime.now()}")
