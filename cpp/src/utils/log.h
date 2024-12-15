#ifndef LOG_H
#define LOG_H

#include <mutex>
#include <string>
#include <iostream>

// Declare the global mutex and logging functions
extern std::mutex g_logMutex;
void logInfo(const std::string &msg);
void logError(const std::string &msg);
void logDebug(const std::string &msg);
void logWarn(const std::string &msg);
#endif // LOG_H
