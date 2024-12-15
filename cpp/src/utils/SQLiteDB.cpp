#include <iostream>
#include <stdexcept>
#include <sqlite3.h>
#include <string>
#include <vector>
#include "log.h"
#include "utils/SQLiteDB.h"
#include "utils/URLItem.h"

// -------------------------------------------------------
// Database Handling (SQLite3)
SQLiteDB::SQLiteDB(const std::string &db_file)
{
    if (sqlite3_open(db_file.c_str(), &db) != SQLITE_OK)
    {
        throw std::runtime_error("Failed to open database");
    }
    this->initialize_tables();
}

SQLiteDB::~SQLiteDB()
{
    if (db)
        sqlite3_close(db);
}

void SQLiteDB::initialize_tables()
{
    const char *create_urls_table = R"SQL(
        CREATE TABLE IF NOT EXISTS urls_to_visit (
            url TEXT PRIMARY KEY,
            url_score REAL,
            page_type TEXT,
            etag TEXT,
            last_modified TEXT,
            status TEXT
        );
        )SQL";

    const char *create_output_table = R"SQL(
        CREATE TABLE IF NOT EXISTS robot_dict (
            domain_path TEXT PRIMARY KEY,
            policy TEXT
        );
        )SQL";

    exec_sql(create_urls_table);
    exec_sql(create_output_table);
}

void SQLiteDB::exec_sql(const std::string &sql)
{
    char *errMsg = nullptr;
    if (sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &errMsg) != SQLITE_OK)
    {
        std::string error = "SQL error: ";
        if (errMsg)
        {
            error += errMsg;
            sqlite3_free(errMsg);
        }
        throw std::runtime_error(error);
    }
}

bool SQLiteDB::have_been_seen(const std::string &url)
{
    std::string sql = "SELECT status FROM urls_to_visit WHERE url=?;";
    sqlite3_stmt *stmt = nullptr;
    bool seen = false;
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, NULL) == SQLITE_OK)
    {
        sqlite3_bind_text(stmt, 1, url.c_str(), -1, SQLITE_TRANSIENT);
        if (sqlite3_step(stmt) == SQLITE_ROW)
        {
            seen = true;
        }
    }
    sqlite3_finalize(stmt);
    return seen;
}

void SQLiteDB::insert_url(const URLItem &item)
{
    std::string sql = "INSERT OR IGNORE INTO urls_to_visit (url, url_score, page_type, etag, last_modified, status) VALUES (?,?,?,?,?,?);";
    sqlite3_stmt *stmt = nullptr;
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, NULL) == SQLITE_OK)
    {
        sqlite3_bind_text(stmt, 1, item.url.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_double(stmt, 2, item.url_score);
        sqlite3_bind_text(stmt, 3, item.page_type.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_null(stmt, 4);
        sqlite3_bind_null(stmt, 5);
        sqlite3_bind_text(stmt, 6, item.status.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
    }
    sqlite3_finalize(stmt);
}

void SQLiteDB::mark_seen(const std::string &url)
{
    std::string sql = "UPDATE urls_to_visit SET status='seen' WHERE url=?;";
    sqlite3_stmt *stmt = nullptr;
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, NULL) == SQLITE_OK)
    {
        sqlite3_bind_text(stmt, 1, url.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
    }
    sqlite3_finalize(stmt);
}

void SQLiteDB::update_error(const std::string &url, const std::string &error_msg)
{
    // In a real scenario, store the error in a dedicated table or log.
    std::string sql = "UPDATE urls_to_visit SET status='error' WHERE url=?;";
    sqlite3_stmt *stmt = nullptr;
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, NULL) == SQLITE_OK)
    {
        sqlite3_bind_text(stmt, 1, url.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
    }
    sqlite3_finalize(stmt);

    logError("Error processing " + url + ": " + error_msg);
}

std::vector<URLItem> SQLiteDB::get_unseen_urls(size_t limit)
{
    std::vector<URLItem> items;

    // SQL query to fetch unseen URLs up to the specified limit
    std::string sql = "SELECT url, url_score, page_type, etag, last_modified, status "
                      "FROM urls_to_visit WHERE status='unseen' LIMIT ?;";

    sqlite3_stmt *stmt = nullptr;

    // Prepare the SQL statement
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, NULL) == SQLITE_OK)
    {
        // Bind the limit parameter
        sqlite3_bind_int(stmt, 1, static_cast<int>(limit));

        // Iterate through the result set
        while (sqlite3_step(stmt) == SQLITE_ROW)
        {
            URLItem item;
            item.url = reinterpret_cast<const char *>(sqlite3_column_text(stmt, 0));
            item.url_score = sqlite3_column_double(stmt, 1);
            item.page_type = reinterpret_cast<const char *>(sqlite3_column_text(stmt, 2));
            item.etag = reinterpret_cast<const char *>(sqlite3_column_text(stmt, 3));
            item.last_modified = reinterpret_cast<const char *>(sqlite3_column_text(stmt, 4));
            item.status = reinterpret_cast<const char *>(sqlite3_column_text(stmt, 5));

            items.push_back(item);
        }
    }

    // Finalize the statement to avoid memory leaks
    sqlite3_finalize(stmt);

    return items;
}


bool SQLiteDB::is_empty()
{
    std::string sql = "SELECT count(*) FROM urls_to_visit WHERE status='unseen';";
    sqlite3_stmt *stmt = nullptr;
    int count = 0;
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, NULL) == SQLITE_OK)
    {
        if (sqlite3_step(stmt) == SQLITE_ROW)
        {
            count = sqlite3_column_int(stmt, 0);
        }
    }
    sqlite3_finalize(stmt);
    return count == 0;
}

