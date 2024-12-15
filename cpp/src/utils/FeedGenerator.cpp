#include "FeedGenerator.h"
#include <libxml/parser.h>
#include <stdexcept>
#include <sstream>
#include "utils/URLItem.h"
#include <libxml2/libxml/xpath.h>
#include <libxml/xpathInternals.h>

std::vector<URLItem> parseUnifiedFeed(std::string docStr) {
    xmlDocPtr doc = xmlReadMemory(docStr.c_str(), docStr.size(), NULL, NULL, 0);
    if (doc == nullptr) {
        throw std::runtime_error("Failed to parse XML document");
    }

    std::vector<URLItem> urlItems = parseUnifiedFeed(doc);
    xmlFreeDoc(doc);
    return urlItems;
}



std::vector<URLItem> parseUnifiedFeed(xmlDocPtr doc) {
    std::vector<URLItem> urlItems;
    xmlXPathContextPtr xpathCtx = xmlXPathNewContext(doc);
    if (xpathCtx == nullptr) {
        throw std::runtime_error("Failed to create XPath context");
    }

    // Register namespaces for XPath queries
    xmlXPathRegisterNs(xpathCtx, BAD_CAST "atom", BAD_CAST "http://www.w3.org/2005/Atom");
    xmlXPathRegisterNs(xpathCtx, BAD_CAST "rss", BAD_CAST "http://purl.org/rss/1.0/");
    xmlXPathRegisterNs(xpathCtx, BAD_CAST "sitemap", BAD_CAST "http://www.sitemaps.org/schemas/sitemap/0.9");

    // Determine feed type by checking root element
    xmlNodePtr root = xmlDocGetRootElement(doc);
    if (root == nullptr) {
        xmlXPathFreeContext(xpathCtx);
        throw std::runtime_error("Empty XML document");
    }

    std::string rootName = (const char*)root->name;
    if (rootName == "urlset") {
        // Sitemap
        xmlXPathObjectPtr xpathObj = xmlXPathEvalExpression(BAD_CAST "//sitemap:url", xpathCtx);
        if (xpathObj == nullptr || xpathObj->nodesetval == nullptr) {
            xmlXPathFreeObject(xpathObj);
            xmlXPathFreeContext(xpathCtx);
            throw std::runtime_error("Failed to evaluate XPath expression for Sitemap");
        }

        for (int i = 0; i < xpathObj->nodesetval->nodeNr; ++i) {
            xmlNodePtr urlNode = xpathObj->nodesetval->nodeTab[i];
            URLItem item;
            xmlXPathObjectPtr locObj = xmlXPathNodeEval(urlNode, BAD_CAST "sitemap:loc", xpathCtx);
            if (locObj != nullptr && locObj->nodesetval != nullptr && locObj->nodesetval->nodeNr > 0) {
                item.url = (const char*)xmlNodeGetContent(locObj->nodesetval->nodeTab[0]);
                xmlXPathFreeObject(locObj);
            }
            xmlXPathObjectPtr lastmodObj = xmlXPathNodeEval(urlNode, BAD_CAST "sitemap:lastmod", xpathCtx);
            if (lastmodObj != nullptr && lastmodObj->nodesetval != nullptr && lastmodObj->nodesetval->nodeNr > 0) {
                item.lastmod = std::optional<std::string>((const char*)xmlXPathCastNodeToString(lastmodObj->nodesetval->nodeTab[0]));
                xmlXPathFreeObject(lastmodObj);
            }
            xmlXPathObjectPtr changefreqObj = xmlXPathNodeEval(urlNode, BAD_CAST "sitemap:changefreq", xpathCtx);
            if (changefreqObj != nullptr && changefreqObj->nodesetval != nullptr && changefreqObj->nodesetval->nodeNr > 0) {
                item.changefreq = std::optional<std::string>((const char*)xmlNodeGetContent(changefreqObj->nodesetval->nodeTab[0]));
                xmlXPathFreeObject(changefreqObj);
            }
            xmlXPathObjectPtr priorityObj = xmlXPathNodeEval(urlNode, BAD_CAST "sitemap:priority", xpathCtx);
            if (priorityObj != nullptr && priorityObj->nodesetval != nullptr && priorityObj->nodesetval->nodeNr > 0) {
                item.sitemap_priority = std::optional<double>(std::stod((const char*)xmlNodeGetContent(priorityObj->nodesetval->nodeTab[0])));
                xmlXPathFreeObject(priorityObj);
            }
            item.status = "unseen";
            urlItems.push_back(item);
        }
        xmlXPathFreeObject(xpathObj);
    } else if (rootName == "rss") {
        // RSS
        xmlXPathObjectPtr xpathObj = xmlXPathEvalExpression(BAD_CAST "//rss:item", xpathCtx);
        if (xpathObj == nullptr || xpathObj->nodesetval == nullptr) {
            xmlXPathFreeObject(xpathObj);
            xmlXPathFreeContext(xpathCtx);
            throw std::runtime_error("Failed to evaluate XPath expression for RSS");
        }

        for (int i = 0; i < xpathObj->nodesetval->nodeNr; ++i) {
            xmlNodePtr itemNode = xpathObj->nodesetval->nodeTab[i];
            URLItem item;
            xmlXPathObjectPtr linkObj = xmlXPathNodeEval(itemNode, BAD_CAST "rss:link", xpathCtx);
            if (linkObj != nullptr && linkObj->nodesetval != nullptr && linkObj->nodesetval->nodeNr > 0) {
                item.url = (const char*)xmlNodeGetContent(linkObj->nodesetval->nodeTab[0]);
                xmlXPathFreeObject(linkObj);
            }
            xmlXPathObjectPtr pubDateObj = xmlXPathNodeEval(itemNode, BAD_CAST "rss:pubDate", xpathCtx);
            if (pubDateObj != nullptr && pubDateObj->nodesetval != nullptr && pubDateObj->nodesetval->nodeNr > 0) {
                item.last_modified = std::optional<std::string>((const char*)xmlNodeGetContent(pubDateObj->nodesetval->nodeTab[0]));
                xmlXPathFreeObject(pubDateObj);
            }
            xmlXPathObjectPtr titleObj = xmlXPathNodeEval(itemNode, BAD_CAST "rss:title", xpathCtx);
            if (titleObj != nullptr && titleObj->nodesetval != nullptr && titleObj->nodesetval->nodeNr > 0) {
                item.page_type = (const char*)xmlNodeGetContent(titleObj->nodesetval->nodeTab[0]);
                xmlXPathFreeObject(titleObj);
            }
            item.status = "unseen";
            urlItems.push_back(item);
        }
        xmlXPathFreeObject(xpathObj);
    } else if (rootName == "feed") {
        // Atom
        xmlXPathObjectPtr xpathObj = xmlXPathEvalExpression(BAD_CAST "//atom:entry", xpathCtx);
        if (xpathObj == nullptr || xpathObj->nodesetval == nullptr) {
            xmlXPathFreeObject(xpathObj);
            xmlXPathFreeContext(xpathCtx);
            throw std::runtime_error("Failed to evaluate XPath expression for Atom");
        }

        for (int i = 0; i < xpathObj->nodesetval->nodeNr; ++i) {
            xmlNodePtr entryNode = xpathObj->nodesetval->nodeTab[i];
            URLItem item;
            xmlXPathObjectPtr hrefObj = xmlXPathNodeEval(entryNode, BAD_CAST "atom:link/@href", xpathCtx);
            if (hrefObj != nullptr && hrefObj->nodesetval != nullptr && hrefObj->nodesetval->nodeNr > 0) {
                item.url = (const char*)xmlNodeGetContent(hrefObj->nodesetval->nodeTab[0]);
                xmlXPathFreeObject(hrefObj);
            }
            xmlXPathObjectPtr updatedObj = xmlXPathNodeEval(entryNode, BAD_CAST "atom:updated", xpathCtx);
            if (updatedObj != nullptr && updatedObj->nodesetval != nullptr && updatedObj->nodesetval->nodeNr > 0) {
                item.last_modified = std::optional<std::string>((const char*)xmlNodeGetContent(updatedObj->nodesetval->nodeTab[0]));
                xmlXPathFreeObject(updatedObj);
            }
            item.page_type = (const char*)xmlXPathCastToString(xmlXPathNodeEval(entryNode, BAD_CAST "atom:title", xpathCtx));
            item.status = "unseen";
            urlItems.push_back(item);
        }
        xmlXPathFreeObject(xpathObj);
    } else {
        xmlXPathFreeContext(xpathCtx);
        throw std::runtime_error("Unsupported feed type");
    }

    xmlXPathFreeContext(xpathCtx);
    return urlItems;
}