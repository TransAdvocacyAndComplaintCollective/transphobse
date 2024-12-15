#ifndef URLITEM_H
#define URLITEM_H

#include <string>
#include <optional>
#include <boost/url.hpp> // Include Boost.URL library

// A helper struct to store URL item details.
struct URLItem
{
    std::string url;
    double url_score = 0.0;
    std::string page_type;
    std::optional<std::string> lastmod;
    std::optional<std::string> changefreq;
    std::optional<double> sitemap_priority;
    std::string status; // "unseen", "seen", "error"
    std::optional<std::string> last_modified;
    std::optional<std::string> etag;
};

// Static method to join a base URL and a relative URL using Boost.URL
std::string join_url(const std::string &base_url, const std::string &relative_url);

#endif // URLITEM_H
