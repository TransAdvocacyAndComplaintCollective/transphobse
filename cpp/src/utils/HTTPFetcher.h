// HTTPFetcher.h

#ifndef HTTPFETCHER_H
#define HTTPFETCHER_H

#include <string>
#include <tuple>
#include <optional>

class HTTPFetcher
{
public:
    HTTPFetcher();
    ~HTTPFetcher();
    std::optional<std::tuple<std::string, std::string>> fetch(const std::string& url);

private:
    static constexpr const char* USER_AGENT = "YourApp/1.0";
    static constexpr const char* DEFAULT_ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
    bool isValidMimeType(const std::string& mime);
    static size_t headerCallback(void* contents, size_t size, size_t nmemb, std::string* headersBuffer);

    static size_t writeCallback(void* contents, size_t size, size_t nmemb, std::string* response);
};

#endif // HTTPFETCHER_H
