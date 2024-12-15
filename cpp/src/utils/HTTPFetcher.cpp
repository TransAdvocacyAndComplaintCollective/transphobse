#include <string>
#include <tuple>
#include <optional>
#include <curl/curl.h>
#include <regex>
#include <iostream> // For logging
#include "HTTPFetcher.h"
#include "log.h"

HTTPFetcher::HTTPFetcher() {
    logInfo("Initializing CURL globally.");
    if (curl_global_init(CURL_GLOBAL_ALL) != 0) {
        logError("Failed to initialize CURL globally.");
        throw std::runtime_error("CURL initialization failed");
    }
}

HTTPFetcher::~HTTPFetcher() {
    logInfo("Cleaning up CURL globally.");
    curl_global_cleanup();
}

size_t HTTPFetcher::writeCallback(void* contents, size_t size, size_t nmemb, std::string* response) {
    if (!response) return 0;
    size_t totalSize = size * nmemb;
    response->append(static_cast<char*>(contents), totalSize);
    return totalSize;
}

size_t HTTPFetcher::headerCallback(void* contents, size_t size, size_t nmemb, std::string* headersBuffer) {
    if (!headersBuffer) return 0;
    size_t totalSize = size * nmemb;
    headersBuffer->append(static_cast<char*>(contents), totalSize);
    return totalSize;
}

std::string filterPrintable(const std::string& input) {
    std::string result;
    std::copy_if(input.begin(), input.end(), std::back_inserter(result),
                 [](unsigned char c) { return std::isprint(c); });
    return result;
}

bool HTTPFetcher::isValidMimeType(const std::string& mime) {
    std::string cleanMime = filterPrintable(mime);
    logDebug("Validating MIME type: " + cleanMime);
    std::regex mimeRegex(R"([a-zA-Z0-9\-\.]+/[a-zA-Z0-9\-\.]+)");
    bool isValid = std::regex_match(cleanMime, mimeRegex);
    if (!isValid) {
        logWarn("Invalid MIME type detected: " + cleanMime);
    }
    return isValid;
}

std::optional<std::tuple<std::string, std::string>> HTTPFetcher::fetch(const std::string& url) {
    logInfo("Fetching URL: " + url);

    CURL* curl = curl_easy_init();
    if (!curl) {
        logError("Failed to initialize CURL.");
        return std::nullopt;
    }

    std::string response;
    std::string headersBuffer;
    struct curl_slist* headers = nullptr;

    // Add headers
    headers = curl_slist_append(headers, ("User-Agent: " + std::string(USER_AGENT)).c_str());
    headers = curl_slist_append(headers, ("Accept: " + std::string(DEFAULT_ACCEPT)).c_str());

    logDebug("Setting up CURL options.");
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, headerCallback);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &headersBuffer);

    logInfo("Performing the HTTP request.");
    CURLcode res = curl_easy_perform(curl);

    long response_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);

    char* content_type = nullptr;
    curl_easy_getinfo(curl, CURLINFO_CONTENT_TYPE, &content_type);

    logDebug("Cleaning up CURL resources.");
    curl_easy_cleanup(curl);
    curl_slist_free_all(headers);

    if (res != CURLE_OK || response_code != 200) {
        logError("Failed to fetch URL: " + url + ", CURLcode: " + std::to_string(res) + ", HTTP code: " + std::to_string(response_code));
        return std::nullopt;
    }

    std::string mime = (content_type && std::all_of(std::string(content_type).begin(), std::string(content_type).end(), [](unsigned char c) { return std::isprint(c); }))
                           ? std::string(content_type)
                           : "application/octet-stream";
    logDebug("Content-Type detected: " + mime);

    if (!isValidMimeType(mime)) {
        logError("Invalid MIME type for URL: " + url + " - MIME: " + mime);
        return std::nullopt;
    }

    logInfo("Successfully fetched URL: " + url);
    return std::make_tuple(response, mime);
}
