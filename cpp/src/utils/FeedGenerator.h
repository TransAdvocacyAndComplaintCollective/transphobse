#ifndef FEEDGENERATOR_H
#define FEEDGENERATOR_H

#include <string>
#include <vector>
#include <libxml/tree.h>
#include "utils/URLItem.h"

std::vector<URLItem> parseUnifiedFeed(xmlDocPtr doc);
std::vector<URLItem> parseUnifiedFeed(std::string docStr) ;
#endif // FEEDGENERATOR_H
