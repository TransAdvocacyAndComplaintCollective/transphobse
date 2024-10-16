from __future__ import annotations

import logging
import warnings
from datetime import datetime, timezone
from decimal import Decimal
from functools import cached_property
from itertools import islice
from random import choice
from threading import Event
from types import TracebackType
from typing import cast
import requests
import primp  # type: ignore

try:
    from lxml.etree import _Element
    from lxml.html import HTMLParser as LHTMLParser
    from lxml.html import document_fromstring

    LXML_AVAILABLE = True
except ImportError:
    LXML_AVAILABLE = False
from duckduckgo_search import DDGS
from duckduckgo_search.exceptions import ConversationLimitException, DuckDuckGoSearchException, RatelimitException, TimeoutException
from duckduckgo_search.utils import (
    _calculate_distance,
    _expand_proxy_tb_alias,
    _extract_vqd,
    _normalize,
    _normalize_url,
    _text_extract_json,
    json_loads,
)

logger = logging.getLogger("duckduckgo_search.DDGS")


class DDGS:
    """DuckDuckgo_search class to get search results from duckduckgo.com."""

    _impersonates = (
        "chrome_100", "chrome_101", "chrome_104", "chrome_105", "chrome_106", "chrome_107", "chrome_108", 
        "chrome_109", "chrome_114", "chrome_116", "chrome_117", "chrome_118", "chrome_119", "chrome_120", 
        "chrome_127", "chrome_128", "chrome_129",
        "safari_ios_16.5", "safari_ios_17.2", "safari_ios_17.4.1", "safari_15.3", "safari_15.5", "safari_15.6.1", 
        "safari_16", "safari_16.5", "safari_17.0", "safari_17.2.1", "safari_17.4.1", "safari_17.5", "safari_18", 
        "safari_ipad_18",
        "edge_101", "edge_122", "edge_127",
    )  # fmt: skip

    def __init__(
        self,
        headers: dict[str, str] | None = None,
        proxy: str | None = None,
        proxies: dict[str, str] | str | None = None,  # deprecated
        timeout: int | None = 10,
    ) -> None:
        """Initialize the DDGS object.

        Args:
            headers (dict, optional): Dictionary of headers for the HTTP client. Defaults to None.
            proxy (str, optional): proxy for the HTTP client, supports http/https/socks5 protocols.
                example: "http://user:pass@example.com:3128". Defaults to None.
            timeout (int, optional): Timeout value for the HTTP client. Defaults to 10.
        """
        self.proxy: str | None = _expand_proxy_tb_alias(proxy)
        assert self.proxy is None or isinstance(self.proxy, str), "proxy must be a str"
        if not proxy and proxies:
            warnings.warn("'proxies' is deprecated, use 'proxy' instead.", stacklevel=1)
            self.proxy = proxies.get("http") or proxies.get("https") if isinstance(proxies, dict) else proxies
        self.headers = headers if headers else {}
        self.headers["Referer"] = "https://duckduckgo.com/"
        self.client = primp.Client(
            headers=self.headers,
            proxy=self.proxy,
            timeout=timeout,
            cookie_store=True,
            referer=True,
            impersonate=choice(self._impersonates),
            follow_redirects=False,
            verify=False,
        )
        self._exception_event = Event()
        self._chat_messages: list[dict[str, str]] = []
        self._chat_tokens_count = 0
        self._chat_vqd: str = ""

    def __enter__(self) -> DDGS:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> None:
        pass

    @cached_property
    def parser(self) -> LHTMLParser:
        """Get HTML parser."""
        return LHTMLParser(remove_blank_text=True, remove_comments=True, remove_pis=True, collect_ids=False)

    def _get_url(
        self,
        method: str,
        url: str,
        params: dict[str, str] | None = None,
        content: bytes | None = None,
        data: dict[str, str] | bytes | None = None,
    ) -> bytes:
        if self._exception_event.is_set():
            raise DuckDuckGoSearchException("Exception occurred in previous call.")
        try:
            # Remove the 'method' argument since requests.get is already 'GET' method
            resp = requests.get(url, params=params, data=data)
            resp.raise_for_status()
            print(resp.text)
        except Exception as ex:
            self._exception_event.set()
            if "time" in str(ex).lower():
                raise TimeoutException(f"{url} {type(ex).__name__}: {ex}") from ex
            raise DuckDuckGoSearchException(f"{url} {type(ex).__name__}: {ex}") from ex

        logger.debug(f"_get_url() {resp.url} {resp.status_code} {len(resp.content)}")
        if resp.status_code == 200:
            return cast(bytes, resp.content)

        self._exception_event.set()
        if resp.status_code in (202, 301, 403):
            raise RatelimitException(f"{resp.url} {resp.status_code} Ratelimit")

        raise DuckDuckGoSearchException(f"{resp.url} returned None. {params=} {content=} {data=}")

    def _get_vqd(self, keywords: str) -> str:
        """Get vqd value for a search query."""
        resp_content = self._get_url("GET", "https://duckduckgo.com", params={"q": keywords})
        return _extract_vqd(resp_content, keywords)

    def chat(self, keywords: str, model: str = "gpt-4o-mini", timeout: int = 30) -> str:
        """Initiates a chat session with DuckDuckGo AI."""
        models_deprecated = {
            "gpt-3.5": "gpt-4o-mini",
            "llama-3-70b": "llama-3.1-70b",
        }
        if model in models_deprecated:
            logger.info(f"{model=} is deprecated, using {models_deprecated[model]}")
            model = models_deprecated[model]
        models = {
            "claude-3-haiku": "claude-3-haiku-20240307",
            "gpt-4o-mini": "gpt-4o-mini",
            "llama-3.1-70b": "meta-llama/Meta-Llama-3.1-70B-Instruct-Turbo",
            "mixtral-8x7b": "mistralai/Mixtral-8x7B-Instruct-v0.1",
        }
        if not self._chat_vqd:
            resp = self.client.get("https://duckduckgo.com/duckchat/v1/status", headers={"x-vqd-accept": "1"})
            self._chat_vqd = resp.headers.get("x-vqd-4", "")

        self._chat_messages.append({"role": "user", "content": keywords})
        self._chat_tokens_count += len(keywords) // 4 if len(keywords) >= 4 else 1

        json_data = {
            "model": models[model],
            "messages": self._chat_messages,
        }
        resp = self.client.post(
            "https://duckduckgo.com/duckchat/v1/chat",
            headers={"x-vqd-4": self._chat_vqd},
            json=json_data,
            timeout=timeout,
        )
        self._chat_vqd = resp.headers.get("x-vqd-4", "")

        data = ",".join(x for line in resp.text.rstrip("[DONE]LIMT_CVRSA\n").split("data:") if (x := line.strip()))
        data = json_loads("[" + data + "]")

        results = []
        for x in data:
            if x.get("action") == "error":
                err_message = x.get("type", "")
                if x.get("status") == 429:
                    raise (
                        ConversationLimitException(err_message)
                        if err_message == "ERR_CONVERSATION_LIMIT"
                        else RatelimitException(err_message)
                    )
                raise DuckDuckGoSearchException(err_message)
            elif message := x.get("message"):
                results.append(message)
        result = "".join(results)

        self._chat_messages.append({"role": "assistant", "content": result})
        self._chat_tokens_count += len(results)
        return result

    def text(
        self,
        keywords: str,
        region: str = "wt-wt",
        safesearch: str = "moderate",
        timelimit: str | None = None,
        backend: str = "api",
        max_results: int | None = None,
    ) -> list[dict[str, str]]:
        """DuckDuckGo text search."""
        if LXML_AVAILABLE is False and backend != "api":
            backend = "api"
            warnings.warn("lxml is not installed. Using backend='api'.", stacklevel=2)

        if backend == "api":
            results = self._text_api(keywords, region, safesearch, timelimit, max_results)
        elif backend == "html":
            results = self._text_html(keywords, region, timelimit, max_results)
        elif backend == "lite":
            results = self._text_lite(keywords, region, timelimit, max_results)
        return results

    def _text_api(
        self,
        keywords: str,
        region: str = "wt-wt",
        safesearch: str = "moderate",
        timelimit: str | None = None,
        max_results: int | None = None,
    ) -> list[dict[str, str]]:
        """DuckDuckGo text search."""
        assert keywords, "keywords is mandatory"

        vqd = self._get_vqd(keywords)

        payload = {
            "q": keywords,
            "kl": region,
            "l": region,
            "p": "",
            "s": "0",
            "df": "",
            "vqd": vqd,
            "bing_market": f"{region[3:]}-{region[:2].upper()}",
            "ex": "",
        }
        safesearch = safesearch.lower()
        if safesearch == "moderate":
            payload["ex"] = "-1"
        elif safesearch == "off":
            payload["ex"] = "-2"
        elif safesearch == "on":
            payload["p"] = "1"
        if timelimit:
            payload["df"] = timelimit

        cache = set()
        results: list[dict[str, str]] = []

        def _text_api_page(s: int) -> list[dict[str, str]]:
            payload["s"] = f"{s}"
            resp_content = self._get_url("GET", "https://links.duckduckgo.com/d.js", params=payload)
            
            page_data = _text_extract_json(resp_content, keywords)
            page_results = []
            for row in page_data:
                href = row.get("u", None)
                if href and href not in cache and href != f"http://www.google.com/search?q={keywords}":
                    cache.add(href)
                    body = _normalize(row["a"])
                    if body:
                        result = {
                            "title": _normalize(row["t"]),
                            "href": _normalize_url(href),
                            "body": body,
                        }
                        page_results.append(result)
            return page_results

        results.extend(_text_api_page(0))
        if max_results:
            slist = range(23, max_results, 50)
            for r in slist:
                results.extend(_text_api_page(r))

        return list(islice(results, max_results))

    def _text_html(
        self,
        keywords: str,
        region: str = "wt-wt",
        timelimit: str | None = None,
        max_results: int | None = None,
    ) -> list[dict[str, str]]:
        """DuckDuckGo text search."""
        assert keywords, "keywords is mandatory"

        payload = {
            "q": keywords,
            "s": "0",
            "o": "json",
            "api": "d.js",
            "vqd": "",
            "kl": region,
            "bing_market": region,
        }
        if timelimit:
            payload["df"] = timelimit
        if max_results and max_results > 20:
            vqd = self._get_vqd(keywords)
            payload["vqd"] = vqd

        cache = set()
        results: list[dict[str, str]] = []

        def _text_html_page(s: int) -> list[dict[str, str]]:
            payload["s"] = f"{s}"
            resp_content = self._get_url("POST", "https://html.duckduckgo.com/html", data=payload)
            if b"No  results." in resp_content:
                return []

            page_results = []
            tree = document_fromstring(resp_content, self.parser)
            elements = tree.xpath("//div[h2]")
            if not isinstance(elements, list):
                return []
            for e in elements:
                if isinstance(e, _Element):
                    hrefxpath = e.xpath("./a/@href")
                    href = str(hrefxpath[0]) if hrefxpath and isinstance(hrefxpath, list) else None
                    if (
                        href
                        and href not in cache
                        and not href.startswith(
                            ("http://www.google.com/search?q=", "https://duckduckgo.com/y.js?ad_domain")
                        )
                    ):
                        cache.add(href)
                        titlexpath = e.xpath("./h2/a/text()")
                        title = str(titlexpath[0]) if titlexpath and isinstance(titlexpath, list) else ""
                        bodyxpath = e.xpath("./a//text()")
                        body = "".join(str(x) for x in bodyxpath) if bodyxpath and isinstance(bodyxpath, list) else ""
                        result = {
                            "title": _normalize(title),
                            "href": _normalize_url(href),
                            "body": _normalize(body),
                        }
                        page_results.append(result)
            return page_results

        results.extend(_text_html_page(0))
        if max_results:
            slist = range(23, max_results, 50)
            for r in slist:
                results.extend(_text_html_page(r))

        return list(islice(results, max_results))

    def _text_lite(
        self,
        keywords: str,
        region: str = "wt-wt",
        timelimit: str | None = None,
        max_results: int | None = None,
    ) -> list[dict[str, str]]:
        """DuckDuckGo text search."""
        assert keywords, "keywords is mandatory"

        payload = {
            "q": keywords,
            "s": "0",
            "o": "json",
            "api": "d.js",
            "vqd": "",
            "kl": region,
            "bing_market": region,
        }
        if timelimit:
            payload["df"] = timelimit

        cache = set()
        results: list[dict[str, str]] = []

        def _text_lite_page(s: int) -> list[dict[str, str]]:
            payload["s"] = f"{s}"
            resp_content = self._get_url("POST", "https://lite.duckduckgo.com/lite/", data=payload)
            if b"No more results." in resp_content:
                return []

            page_results = []
            tree = document_fromstring(resp_content, self.parser)
            elements = tree.xpath("//table[last()]//tr")
            if not isinstance(elements, list):
                return []

            for e in elements:
                if isinstance(e, _Element):
                    hrefxpath = e.xpath(".//a//@href")
                    href = str(hrefxpath[0]) if hrefxpath and isinstance(hrefxpath, list) else None
                    if href is None or href in cache:
                        continue
                    cache.add(href)
                    titlexpath = e.xpath(".//a//text()")
                    title = str(titlexpath[0]) if titlexpath and isinstance(titlexpath, list) else ""
                    bodyxpath = e.xpath(".//td[@class='result-snippet']//text()")
                    body = "".join(str(x) for x in bodyxpath).strip() if bodyxpath and isinstance(bodyxpath, list) else ""
                    if href:
                        result = {
                            "title": _normalize(title),
                            "href": _normalize_url(href),
                            "body": _normalize(body),
                        }
                        page_results.append(result)
            return page_results

        results.extend(_text_lite_page(0))
        if max_results:
            slist = range(23, max_results, 50)
            for r in slist:
                results.extend(_text_lite_page(r))

        return list(islice(results, max_results))

# Additional search methods (like images, videos, etc.) would be updated similarly.
ddgs = DDGS()
search_results = ddgs.text(
    keywords="Python programming",  # the search query
    region="us-en",                 # region for the search results (default: "wt-wt")
    safesearch="moderate",          # safesearch setting (default: "moderate")
    timelimit=None,                 # time filter: d (day), w (week), m (month), y (year) (default: None)
    backend="api",                  # backend type: api, html, or lite (default: "api")
    max_results=10                  # maximum number of search results to return
)