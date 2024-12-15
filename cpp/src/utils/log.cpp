#include "log.h"

// Define the global mutex and logging functions
std::mutex g_logMutex;

void logInfo(const std::string &msg) {
    std::lock_guard<std::mutex> lk(g_logMutex);
    std::cerr << "[INFO] " << msg << "\n";
}

void logError(const std::string &msg) {
    std::lock_guard<std::mutex> lk(g_logMutex);
    std::cerr << "[ERROR] " << msg << "\n";
}
void logDebug(const std::string &msg){
    std::lock_guard<std::mutex> lk(g_logMutex);
    std::cerr << "[DEBUG] " << msg<< "\n";
}
void logWarn(const std::string &msg){
    std::lock_guard<std::mutex> lk(g_logMutex);
    std::cerr << "[WARN] " << msg<< "\n";
}