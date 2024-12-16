#ifndef CRAWLER_H
#define CRAWLER_H

#include <string>
#include <vector>
#include <queue>
#include <mutex>
#include <thread>
#include <regex>
#include <condition_variable>
#include <unordered_map>
#include <iostream>
#include <algorithm>
#include "utils/SQLiteDB.h"
#include "utils/HTTPFetcher.h"
#include "utils/CSVWriter.h"
#include "utils/URLItem.h"
#include "utils/log.h"
#include "utils/FeedGenerator.h"

class Crawler
{
public:
Crawler(
    const std::vector<std::string> &startUrls,
    const std::string &dbName,
    const std::string &outputCsv,
    const std::vector<std::string> &keywords = {},
    const std::vector<std::string> &antiKeywords = {},
    const std::vector<std::string> &feeds = {},
    const std::vector<std::string> &allowed_subdirs_cruel = {},
    const std::vector<std::string> &exclude_subdirs_cruel = {},
    const std::vector<std::string> &exclude_scrape_subdirs = {});

    ~Crawler();

    void start();
    void stop();

private:
    SQLiteDB db;
    CSVWriter csvWriter;
    HTTPFetcher fetcher;
    std::queue<URLItem> urlQueue;
    std::mutex queueMutex;
    std::condition_variable queueCondition;
    bool stopCrawling;

    std::vector<std::string> startUrls;
    std::vector<std::string> keywords;
    std::vector<std::string> antiKeywords;
    std::vector<std::string> feeds;
    std::vector<std::string> allowed_subdirs_cruel;
    std::vector<std::string> exclude_subdirs_cruel;
    std::vector<std::string> exclude_scrape_subdirs;

    void initializeOutputCsv();
    void crawlWorker();
    void processUrl(const URLItem &urlItem);
    bool isAllowedByFilters(const std::string &url);
    void analyzeWebpageContent(const std::string &content, const URLItem &urlItem);
    std::pair<double, std::vector<std::string>> analyzeKeywords(const std::string &content, const std::vector<std::string> &keywords);
    std::vector<std::string> extractLinks(const std::string &content, const std::string &baseUrl);
    std::string joinStrings(const std::vector<std::string> &vec, const std::string &delimiter);
    std::vector<URLItem> parseUnifiedFeed(const std::string &content); // Assuming this method exists
    std::string join_url(const std::string &base, const std::string &relative); // Assuming this method exists
};

#endif // CRAWLER_H
