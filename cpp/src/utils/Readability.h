#include <iostream>
#include <vector>
#include <string>
#include <regex>
#include <libxml/HTMLparser.h>
#include <libxml/xpath.h>

// Define ReadabilityOptions structure
struct ReadabilityOptions {
    bool debug = false;
    int maxElemsToParse = 0;
    int nbTopCandidates = 5;
    int charThreshold = 500;
    std::vector<std::string> classesToPreserve = {"page"};
    bool keepClasses = false;
    std::string serializer = ""; // Placeholder, implement as needed
    bool disableJSONLD = false;
    std::string allowedVideoRegex = "//(www\\.)?((dailymotion|youtube|youtube-nocookie|player\\.vimeo|v\\.qq)\\.com|(archive|upload\\.wikimedia)\\.org|player\\.twitch\\.tv)";
    double linkDensityModifier = 0.0;
};

// Define ReadabilityMetadata structure
struct ReadabilityMetadata {
    std::string title;
    std::string byline;
    std::string excerpt;
    std::string siteName;
    std::string publishedTime;
};

// Define ReadabilityResult structure
struct ReadabilityResult {
    std::string title;
    std::string byline;
    std::string dir;
    std::string lang;
    std::string content;
    std::string textContent;
    int length;
    std::string excerpt;
    std::string siteName;
    std::string publishedTime;
};

// Define the Readability class
class Readability {
public:
    // Constructor
    Readability(htmlDocPtr doc, ReadabilityOptions options);

    // Public method to parse the document
    ReadabilityResult parse();

private:
    // Member variables
    htmlDocPtr _doc;
    ReadabilityOptions _options;
    ReadabilityMetadata _metadata;
    std::string _articleTitle;
    std::string _articleByline;
    std::string _articleDir;
    std::string _articleSiteName;
    std::vector<std::pair<xmlNodePtr, int>> _attempts;

    // Flags
    int _flags;
    const int FLAG_STRIP_UNLIKELYS = 0x1;
    const int FLAG_WEIGHT_CLASSES = 0x2;
    const int FLAG_CLEAN_CONDITIONALLY = 0x4;

    // Regular expressions
    std::regex unlikelyCandidates;
    std::regex okMaybeItsACandidate;
    std::regex positive;
    std::regex negative;
    std::regex extraneous;
    std::regex bylineRegex;
    std::regex replaceFonts;
    std::regex normalize;
    std::regex videos;
    std::regex shareElements;
    std::regex nextLink;
    std::regex prevLink;
    std::regex tokenize;
    std::regex whitespace;
    std::regex hasContent;
    std::regex hashUrl;
    std::regex srcsetUrl;
    std::regex b64DataUrl;
    std::regex commas;
    std::regex jsonLdArticleTypes;
    std::regex adWords;
    std::regex loadingWords;

    // Helper methods
    void _removeNodes(xmlNodeSetPtr nodeList, bool (*filterFn)(xmlNodePtr, int, xmlNodeSetPtr) = nullptr);
    void _replaceNodeTags(xmlNodeSetPtr nodeList, const char* newTagName);
    void _forEachNode(xmlNodeSetPtr nodeList, void (*fn)(xmlNodePtr));
    xmlNodePtr _findNode(xmlNodeSetPtr nodeList, bool (*fn)(xmlNodePtr));
    bool _someNode(xmlNodeSetPtr nodeList, bool (*fn)(xmlNodePtr));
    bool _everyNode(xmlNodeSetPtr nodeList, bool (*fn)(xmlNodePtr));
    xmlNodeSetPtr _getAllNodesWithTag(xmlNodePtr node, const std::vector<std::string>& tagNames);
    void _cleanClasses(xmlNodePtr node);
    void _fixRelativeUris(xmlNodePtr articleContent);
    void _simplifyNestedElements(xmlNodePtr articleContent);
    std::string _getArticleTitle();
    ReadabilityMetadata _getJSONLD(xmlDocPtr doc);
    ReadabilityMetadata _getArticleMetadata(ReadabilityMetadata jsonld);
    bool _isSingleImage(xmlNodePtr node);
    void _unwrapNoscriptImages(xmlDocPtr doc);
    void _removeScripts(xmlDocPtr doc);
    bool _hasSingleTagInsideElement(xmlNodePtr element, const std::string& tag);
    bool _isElementWithoutContent(xmlNodePtr node);
    bool _hasChildBlockElement(xmlNodePtr element);
    bool _isPhrasingContent(xmlNodePtr node);
    bool _isWhitespace(xmlNodePtr node);
    std::string _getInnerText(xmlNodePtr node, bool normalizeSpaces = true);
    int _getCharCount(xmlNodePtr node, const std::string& s = ",");
    void _cleanStyles(xmlNodePtr node);
    double _getLinkDensity(xmlNodePtr element);
    int _getClassWeight(xmlNodePtr e);
    void _clean(xmlNodePtr e, const std::string& tag);
    bool _hasAncestorTag(xmlNodePtr node, const std::string& tagName, int maxDepth = 3, bool (*filterFn)(xmlNodePtr) = nullptr);
    std::pair<int, int> _getRowAndColumnCount(xmlNodePtr table);
    void _markDataTables(xmlNodePtr root);
    void _fixLazyImages(xmlNodePtr root);
    double _textSimilarity(const std::string& textA, const std::string& textB);
    bool _isValidByline(xmlNodePtr node, const std::string& matchString);
    std::vector<xmlNodePtr> _getNodeAncestors(xmlNodePtr node, int maxDepth = 0);
    xmlNodePtr _grabArticle(xmlNodePtr page);
    void _postProcessContent(xmlNodePtr articleContent);
    void log(const std::string& message);
};