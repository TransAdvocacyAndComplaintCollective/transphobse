#ifndef SQLITEDB_H
#define SQLITEDB_H

#include <string>
#include <vector>
#include <sqlite3.h>
#include "URLItem.h"

class SQLiteDB {
public:
    SQLiteDB(const std::string &db_file);
    ~SQLiteDB();
    
    bool have_been_seen(const std::string &url);
    void insert_url(const URLItem &item);
    void mark_seen(const std::string &url);
    void update_error(const std::string &url, const std::string &error_msg);
    std::vector<URLItem> get_unseen_urls(size_t limit = 100);
    bool is_empty();


private:
    void initialize_tables();
    void exec_sql(const std::string &sql);

    sqlite3 *db; // <-- Make sure this is declared
};

#endif
