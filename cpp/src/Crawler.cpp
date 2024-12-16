#include "Crawler.h"


Crawler::Crawler(
    const std::vector<std::string> &startUrls,
    const std::string &dbName,
    const std::string &outputCsv,
    const std::vector<std::string> &keywords,
    const std::vector<std::string> &antiKeywords,
    const std::vector<std::string> &feeds,
    const std::vector<std::string> &allowed_subdirs_cruel,
    const std::vector<std::string> &exclude_subdirs_cruel,
    const std::vector<std::string> &exclude_scrape_subdirs)
    : db(dbName),
      csvWriter(outputCsv),
      stopCrawling(false),
      startUrls(startUrls),
      keywords(keywords),
      antiKeywords(antiKeywords),
      feeds(feeds),
      allowed_subdirs_cruel(allowed_subdirs_cruel),
      exclude_subdirs_cruel(exclude_subdirs_cruel),
      exclude_scrape_subdirs(exclude_scrape_subdirs)
{
    for (const auto &url : startUrls)
    {
        URLItem item;
        item.url = url;
        item.page_type = "webpage";
        urlQueue.push(item);
    }
}



Crawler::~Crawler()
{
    stop();
}

void Crawler::start()
{
    initializeOutputCsv();

    std::vector<std::thread> workers;
    unsigned int numThreads = std::thread::hardware_concurrency();
    if (numThreads == 0)
    {
        numThreads = 4; // Default to 4 threads if unable to detect
    }

    for (unsigned int i = 0; i < numThreads; ++i)
    {
        workers.emplace_back(&Crawler::crawlWorker, this);
    }

    for (auto &worker : workers)
    {
        if (worker.joinable())
        {
            worker.join();
        }
    }
}

void Crawler::stop()
{
    {
        std::unique_lock<std::mutex> lock(queueMutex);
        stopCrawling = true;
    }
    queueCondition.notify_all();
}

void Crawler::initializeOutputCsv()
{
    csvWriter.initialize();
}

void Crawler::crawlWorker()
{
    while (true)
    {
        URLItem urlItem;
        {
            std::unique_lock<std::mutex> lock(queueMutex);
            queueCondition.wait(lock, [this]
                                { return stopCrawling || !urlQueue.empty(); });

            if (stopCrawling && urlQueue.empty())
            {
                return;
            }

            urlItem = urlQueue.front();
            urlQueue.pop();
        }

        processUrl(urlItem);
    }
}

void Crawler::processUrl(const URLItem &urlItem)
{
    if (db.have_been_seen(urlItem.url))
    {
        return;
    }

    if (!isAllowedByFilters(urlItem.url))
    {
        logInfo("URL excluded by filters: " + urlItem.url);
        return;
    }

    logInfo("Processing URL: " + urlItem.url);
    auto fetchResult = fetcher.fetch(urlItem.url);
    if (!fetchResult.has_value())
    {
        logError("Failed to fetch: " + urlItem.url);
        db.update_error(urlItem.url, "Fetch failed");
        return;
    }

    const auto &[content, mimeType] = fetchResult.value();
    if ((mimeType.find("html") != std::string::npos) || urlItem.page_type == "webpage")
    {
        analyzeWebpageContent(content, urlItem);
    }
    else if (((mimeType.find("xml") != std::string::npos) && (urlItem.page_type == "feed")) || (mimeType.find("rss") != std::string::npos))
    {
        // Process XML feed
        logInfo("Processing XML feed: " + urlItem.url);
        std::vector<URLItem> items = parseUnifiedFeed(content);
        for (const auto &item : items)
        {
            logDebug("Feed item: " + item.url);
            if (!db.have_been_seen(item.url))
            {
                {
                    std::unique_lock<std::mutex> lock(queueMutex);
                    urlQueue.push(item);
                    queueCondition.notify_one();
                }
            }
        }
    }
    else
    {
        logError("Unsupported MIME type: " + mimeType);
        db.update_error(urlItem.url, "Unsupported MIME type");
    }

    db.mark_seen(urlItem.url);
}

bool Crawler::isAllowedByFilters(const std::string &url)
{
    // Check allowed subdirectories
    if (!allowed_subdirs_cruel.empty() &&
        std::none_of(allowed_subdirs_cruel.begin(), allowed_subdirs_cruel.end(), [&url](const std::string &prefix)
                     { return url.find(prefix) == 0; }))
    {
        return false;
    }

    // Check excluded subdirectories
    if (std::any_of(exclude_subdirs_cruel.begin(), exclude_subdirs_cruel.end(), [&url](const std::string &prefix)
                    { return url.find(prefix) == 0; }))
    {
        return false;
    }

    // Check excluded scrape subdirectories
    if (std::any_of(exclude_scrape_subdirs.begin(), exclude_scrape_subdirs.end(), [&url](const std::string &prefix)
                    { return url.find(prefix) == 0; }))
    {
        return false;
    }

    return true;
}

void Crawler::analyzeWebpageContent(const std::string &content, const URLItem &urlItem)
{
    std::vector<std::string> links = extractLinks(content, urlItem.url);

    for (const auto &link : links)
    {
        std::string normalizedUrl = join_url(urlItem.url, link);
        if (isAllowedByFilters(normalizedUrl) && !db.have_been_seen(normalizedUrl))
        {
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

    std::pair<double, std::vector<std::string>> item = analyzeKeywords(content, keywords);
    double score = item.first;
    std::vector<std::string> foundKeywords = item.second;

    if (score > 0)
    {
        std::map<std::string, std::string> row = {
            {"URL", urlItem.url},
            {"Score", std::to_string(score)},
            {"Keywords", joinStrings(foundKeywords, ", ")}};
        csvWriter.write_row(row);
    }
}

std::pair<double, std::vector<std::string>> Crawler::analyzeKeywords(
    const std::string &content, const std::vector<std::string> &keywords)
{
    double score = 0.0;
    std::vector<std::string> foundKeywords;

    for (const auto &keyword : keywords)
    {
        if (content.find(keyword) != std::string::npos)
        {
            score += 1.0;
            foundKeywords.push_back(keyword); // Track the matched keyword
        }
    }

    // Return the score and the list of found keywords
    return {score, foundKeywords};
}

std::vector<std::string> Crawler::extractLinks(const std::string &content, const std::string &baseUrl)
{
    std::vector<std::string> links;
    std::regex linkRegex(R"(href\s*=\s*\"([^\"]+)\")", std::regex::icase);
    auto linksBegin = std::sregex_iterator(content.begin(), content.end(), linkRegex);
    auto linksEnd = std::sregex_iterator();

    for (auto it = linksBegin; it != linksEnd; ++it)
    {
        std::smatch match = *it;
        std::string link = match[1].str();
        if (!link.empty())
        {
            links.push_back(link);
        }
    }
    return links;
}

std::string Crawler::joinStrings(const std::vector<std::string> &vec, const std::string &delimiter)
{
    std::string result;
    for (size_t i = 0; i < vec.size(); ++i)
    {
        result += vec[i];
        if (i < vec.size() - 1)
        {
            result += delimiter;
        }
    }
    return result;
}

// Placeholder implementations for assumed methods.
// You should replace these with actual implementations.

std::vector<URLItem> Crawler::parseUnifiedFeed(const std::string &content)
{
    // Implement feed parsing logic here
    return {};
}

std::string Crawler::join_url(const std::string &base, const std::string &relative)
{
    // Implement URL joining logic here
    // This is a simple placeholder
    if (relative.find("http://") == 0 || relative.find("https://") == 0)
    {
        return relative;
    }
    // Handle relative URLs
    // For simplicity, append relative to base
    return base + "/" + relative;
}


int main(int argc, char *argv[])
{
    std::vector<std::string> startUrls = {"https://www.bbc.co.uk/news"};
    std::vector<std::string> feeds = {
        "https://feeds.bbci.co.uk/news/rss.xml",
        "https://feeds.bbci.co.uk/news/uk/rss.xml",
        "https://feeds.bbci.co.uk/news/business/rss.xml"};

    std::vector<std::string> allowed_subdirs_cruel = {"https://www.bbc.co.uk/news/"};
    std::vector<std::string> exclude_subdirs_cruel = {"https://www.bbc.co.uk/news/world-"};
    std::vector<std::string> exclude_scrape_subdirs = {"https://www.bbc.co.uk/news/resources/"};

    std::string dbName = "crawler.db";
    std::string outputCsv = "output.csv";

    std::vector<std::string> keywords = {"transgender", "LGBTQ"};
    std::vector<std::string> antiKeywords = {};

    Crawler crawler(startUrls, dbName, outputCsv, keywords, antiKeywords, feeds, allowed_subdirs_cruel, exclude_subdirs_cruel, exclude_scrape_subdirs);
    crawler.start();

    return 0;
}
