#include <libxml/HTMLparser.h>
#include <libxml/tree.h>
#include <iostream>
#include <regex>
#include <string>

class NoscriptUnwrapper {
public:
    void unwrapNoscriptImages(htmlDocPtr doc) {
        if (!doc) return;

        xmlNodePtr root = xmlDocGetRootElement(doc);
        if (!root) return;

        removePlaceholderImages(root);
        replaceNoscriptImages(root);
    }

private:
    void removePlaceholderImages(xmlNodePtr node) {
        for (xmlNodePtr cur = node; cur; cur = cur->next) {
            if (cur->type == XML_ELEMENT_NODE && xmlStrcmp(cur->name, BAD_CAST "img") == 0) {
                if (!hasValidImageAttributes(cur)) {
                    xmlNodePtr next = cur->next;
                    xmlUnlinkNode(cur);
                    xmlFreeNode(cur);
                    cur = next;
                }
            } else {
                removePlaceholderImages(cur->children);
            }
        }
    }

    bool hasValidImageAttributes(xmlNodePtr imgNode) {
        for (xmlAttrPtr attr = imgNode->properties; attr; attr = attr->next) {
            std::string attrName = reinterpret_cast<const char*>(attr->name);
            std::string attrValue = reinterpret_cast<const char*>(xmlNodeGetContent(attr->children));

            if (attrName == "src" || attrName == "srcset" || attrName == "data-src" || attrName == "data-srcset") {
                return true;
            }

            if (std::regex_search(attrValue, std::regex(R"((\.jpg|\.jpeg|\.png|\.webp)$)", std::regex::icase))) {
                return true;
            }
        }
        return false;
    }

    void replaceNoscriptImages(xmlNodePtr node) {
        for (xmlNodePtr cur = node; cur; cur = cur->next) {
            if (cur->type == XML_ELEMENT_NODE && xmlStrcmp(cur->name, BAD_CAST "noscript") == 0) {
                if (!isSingleImageNoscript(cur)) {
                    continue;
                }

                xmlNodePtr prevSibling = cur->prev;
                if (prevSibling && isSingleImageElement(prevSibling)) {
                    replaceImageWithNoscriptContent(prevSibling, cur);
                }

                xmlNodePtr next = cur->next;
                xmlUnlinkNode(cur);
                xmlFreeNode(cur);
                cur = next;
            } else {
                replaceNoscriptImages(cur->children);
            }
        }
    }

    bool isSingleImageNoscript(xmlNodePtr noscriptNode) {
        if (!noscriptNode->children || !noscriptNode->children->next) {
            xmlNodePtr child = noscriptNode->children;
            return child && child->type == XML_ELEMENT_NODE && xmlStrcmp(child->name, BAD_CAST "img") == 0;
        }
        return false;
    }

    bool isSingleImageElement(xmlNodePtr element) {
        if (element->type == XML_ELEMENT_NODE && xmlStrcmp(element->name, BAD_CAST "img") == 0) {
            return true;
        }

        if (element->type == XML_ELEMENT_NODE) {
            int imgCount = 0;
            for (xmlNodePtr child = element->children; child; child = child->next) {
                if (child->type == XML_ELEMENT_NODE && xmlStrcmp(child->name, BAD_CAST "img") == 0) {
                    imgCount++;
                }
            }
            return imgCount == 1;
        }

        return false;
    }

    void replaceImageWithNoscriptContent(xmlNodePtr imgElement, xmlNodePtr noscriptNode) {
        xmlNodePtr newContent = parseNoscriptContent(noscriptNode);
        if (!newContent) return;

        for (xmlAttrPtr attr = imgElement->properties; attr; attr = attr->next) {
            std::string attrName = reinterpret_cast<const char*>(attr->name);
            std::string attrValue = reinterpret_cast<const char*>(xmlNodeGetContent(attr->children));

            if (attrValue.empty() || (attrName != "src" && attrName != "srcset" && !std::regex_search(attrValue, std::regex(R"((\.jpg|\.jpeg|\.png|\.webp)$)", std::regex::icase)))) {
                continue;
            }

            if (!xmlHasProp(newContent, BAD_CAST attrName.c_str())) {
                xmlNewProp(newContent, BAD_CAST attrName.c_str(), BAD_CAST attrValue.c_str());
            } else {
                std::string newAttrName = "data-old-" + attrName;
                xmlNewProp(newContent, BAD_CAST newAttrName.c_str(), BAD_CAST attrValue.c_str());
            }
        }

        xmlReplaceNode(imgElement, newContent);
    }

    xmlNodePtr parseNoscriptContent(xmlNodePtr noscriptNode) {
        xmlNodePtr tmp = htmlNewDocNode(noscriptNode->doc, NULL, BAD_CAST "div", NULL);
        if (!tmp) return NULL;

        htmlParseDoc((xmlChar*)noscriptNode->children->content, "UTF-8");
        return tmp->children;
    }
};

int main() {
    // Load HTML document
    htmlDocPtr doc = htmlReadFile("example.html", NULL, HTML_PARSE_RECOVER | HTML_PARSE_NOERROR | HTML_PARSE_NOWARNING);
    if (!doc) {
        std::cerr << "Failed to parse document" << std::endl;
        return 1;
    }

    NoscriptUnwrapper unwrapper;
    unwrapper.unwrapNoscriptImages(doc);

    // Save the modified document
    htmlSaveFile("output.html", doc);
    xmlFreeDoc(doc);
    xmlCleanupParser();
    return 0;
}
