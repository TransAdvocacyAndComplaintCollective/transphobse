#include <string>
#include <optional>
#include <boost/url.hpp> 

// Static method to join a base URL and a relative URL using Boost.URL
std::string join_url(const std::string &base_url, const std::string &relative_url)
{
    try
    {
        // Parse the base URL.
        boost::urls::url base = boost::urls::parse_uri(base_url).value();

        // Parse the relative URL into a url_view.
        boost::urls::url_view relative = boost::urls::parse_uri_reference(relative_url).value();

        // Resolve the relative URL against the base URL.
        boost::urls::url resolved_url;
        auto resolved_result = boost::urls::resolve(base, relative, resolved_url);
        if (!resolved_result)
        {
            throw std::runtime_error(resolved_result.error().message());
        }

        return resolved_url.buffer(); // Return the joined URL as a string.
    }
    catch (const std::exception &e)
    {
        // Handle invalid URLs or resolution errors.
        return "Error: " + std::string(e.what());
    }
}

// Check if a URL is a subset of another URL
bool is_sub_url(const std::string& base_url, const std::string& child_url) {
    try {
        boost::urls::url base = boost::urls::parse_uri(base_url).value();
        boost::urls::url child = boost::urls::parse_uri(child_url).value();

        return child.buffer().find(base.buffer()) == 0;
    } catch (const std::exception &e) {
        return false; // Invalid URLs
    }
}
