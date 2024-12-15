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

class Crawler {
public:
    Crawler(
        const std::vector<std::string>& startUrls,
        const std::string& dbName,
        const std::string& outputCsv,
        const std::vector<std::string>& keywords = {},
        const std::vector<std::string>& antiKeywords = {},
        const std::vector<std::string>& feeds = {},
        const std::vector<std::string>& allowed_subdirs_cruel = {},
        const std::vector<std::string>& exclude_subdirs_cruel = {},
        const std::vector<std::string>& exclude_scrape_subdirs = {}
    ) : db(dbName), csvWriter(outputCsv), stopCrawling(false) {
        this->startUrls = startUrls;
        this->keywords = keywords;
        this->antiKeywords = antiKeywords;
        this->feeds = feeds;
        this->allowed_subdirs_cruel = allowed_subdirs_cruel;
        this->exclude_subdirs_cruel = exclude_subdirs_cruel;
        this->exclude_scrape_subdirs = exclude_scrape_subdirs;

        for (const auto& url : startUrls) {
            URLItem item;
            item.url = url;
            item.page_type = "webpage";
            urlQueue.push(item);
        }
    }

    ~Crawler() {
        stop();
    }

    void start() {
        initializeOutputCsv();

        std::vector<std::thread> workers;
        for (size_t i = 0; i < std::thread::hardware_concurrency(); ++i) {
            workers.emplace_back(&Crawler::crawlWorker, this);
        }

        for (auto& worker : workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

    void stop() {
        {
            std::unique_lock<std::mutex> lock(queueMutex);
            stopCrawling = true;
        }
        queueCondition.notify_all();
    }

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

    void initializeOutputCsv() {
        csvWriter.initialize();
    }

    void crawlWorker() {
        while (true) {
            URLItem urlItem;
            {
                std::unique_lock<std::mutex> lock(queueMutex);
                queueCondition.wait(lock, [this] { return stopCrawling || !urlQueue.empty(); });

                if (stopCrawling && urlQueue.empty()) {
                    return;
                }

                urlItem = urlQueue.front();
                urlQueue.pop();
            }

            processUrl(urlItem);
        }
    }

    void processUrl(const URLItem& urlItem) {
        if (db.have_been_seen(urlItem.url)) {
            return;
        }

        // if (!isAllowedByFilters(urlItem.url)) {
        //     logInfo("URL excluded by filters: " + urlItem.url);
        //     return;
        // }

        logInfo("Processing URL: " + urlItem.url);
        auto fetchResult = fetcher.fetch(urlItem.url);
        if (!fetchResult.has_value()) {
            logError("Failed to fetch: " + urlItem.url);
            db.update_error(urlItem.url, "Fetch failed");
            return;
        }

        const auto& [content, mimeType] = fetchResult.value();
        if ((mimeType.find("html") != std::string::npos) || urlItem.page_type == "webpage") {
            analyzeWebpageContent(content, urlItem);
        } 
        else if ((mimeType.find("xml") != std::string::npos) && (urlItem.page_type == "feed") || (mimeType.find("rss") != std::string::npos)) {
            // Process XML feed
            logInfo("Processing XML feed: " + urlItem.url);
            std::vector<URLItem> items = parseUnifiedFeed(content);
            for (const auto& item : items) {
                logDebug("Feed item: " + item.url);
                if (!db.have_been_seen(item.url)) {
                    {
                        std::unique_lock<std::mutex> lock(queueMutex);
                        urlQueue.push(item);
                        queueCondition.notify_one();
                    }
                }
            }

            
        }
        else {
            logError("Unsupported MIME type: " + mimeType);
            db.update_error(urlItem.url, "Unsupported MIME type");
        }

        db.mark_seen(urlItem.url);
    }

    bool isAllowedByFilters(const std::string& url) {
        // Check allowed subdirectories
        if (!allowed_subdirs_cruel.empty() &&
            std::none_of(allowed_subdirs_cruel.begin(), allowed_subdirs_cruel.end(), [&url](const std::string& prefix) {
                return url.find(prefix) == 0;
            })) {
            return false;
        }

        // Check excluded subdirectories
        if (std::any_of(exclude_subdirs_cruel.begin(), exclude_subdirs_cruel.end(), [&url](const std::string& prefix) {
                return url.find(prefix) == 0;
            })) {
            return false;
        }

        // Check excluded scrape subdirectories
        if (std::any_of(exclude_scrape_subdirs.begin(), exclude_scrape_subdirs.end(), [&url](const std::string& prefix) {
                return url.find(prefix) == 0;
            })) {
            return false;
        }

        return true;
    }

    void analyzeWebpageContent(const std::string& content, const URLItem& urlItem) {
        std::vector<std::string> links = extractLinks(content, urlItem.url);

        for (const auto& link : links) {
            std::string normalizedUrl = join_url(urlItem.url, link);
            if (isAllowedByFilters(normalizedUrl) && !db.have_been_seen(normalizedUrl)) {
                URLItem newItem;
                newItem.url = normalizedUrl;
                newItem.page_type = "webpage";

                {
                    std::unique_lock<std::mutex> lock(queueMutex);
                    urlQueue.push(newItem);
                    queueCondition.notify_one();
                }
            }
        }

        double score = analyzeKeywords(content);
        if (score > 0) {
            std::map<std::string, std::string> row = {
                {"URL", urlItem.url},
                {"Score", std::to_string(score)}
            };
            csvWriter.write_row(row);
        }
    }

    double analyzeKeywords(const std::string& content) {
        double score = 0.0;
        for (const auto& keyword : keywords) {
            if (content.find(keyword) != std::string::npos) {
                score += 1.0;
            }
        }
        for (const auto& antiKeyword : antiKeywords) {
            if (content.find(antiKeyword) != std::string::npos) {
                score -= 1.0;
            }
        }
        return score;
    }

    std::vector<std::string> extractLinks(const std::string& content, const std::string& baseUrl) {
        std::vector<std::string> links;
        std::regex linkRegex(R"(href=\"([^\"]+)\")");
        auto linksBegin = std::sregex_iterator(content.begin(), content.end(), linkRegex);
        auto linksEnd = std::sregex_iterator();

        for (auto it = linksBegin; it != linksEnd; ++it) {
            std::smatch match = *it;
            std::string link = match[1].str();
            if (!link.empty()) {
                links.push_back(link);
            }
        }
        return links;
    }
};

int main(int argc, char* argv[]) {
    std::vector<std::string> startUrls = { "https://www.bbc.co.uk/news" };
    std::vector<std::string> feeds = {
        "https://feeds.bbci.co.uk/news/rss.xml",
        "https://feeds.bbci.co.uk/news/uk/rss.xml",
        "https://feeds.bbci.co.uk/news/business/rss.xml"
    };

    std::vector<std::string> allowed_subdirs_cruel = {"https://www.bbc.co.uk/news/"};
    std::vector<std::string> exclude_subdirs_cruel = {"https://www.bbc.co.uk/news/world-"};
    std::vector<std::string> exclude_scrape_subdirs = {"https://www.bbc.co.uk/news/resources/"};

    std::string dbName = "crawler.db";
    std::string outputCsv = "output.csv";

    std::vector<std::string> keywords = {"trans", "LGBTQ"};
    std::vector<std::string> antiKeywords = {"hate", "bias"};

    Crawler crawler(startUrls, dbName, outputCsv, keywords, antiKeywords, feeds, allowed_subdirs_cruel, exclude_subdirs_cruel, exclude_scrape_subdirs);
    crawler.start();

    return 0;
}

#endif // CRAWLER_H
