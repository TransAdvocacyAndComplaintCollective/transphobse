#ifndef CSVWRITER_H
#define CSVWRITER_H

#include <string>
#include <vector>
#include <map>
#include <fstream>
#include <mutex>

class CSVWriter {
public:
    explicit CSVWriter(const std::string &file_path);
    ~CSVWriter();

    void initialize();
    void write_row(const std::map<std::string, std::string> &row);
    void flush();

private:
    std::string file_path;
    std::ofstream out_stream;
    std::mutex write_mutex;
    std::vector<std::string> headers;
    bool initialized;

    void write_headers(const std::map<std::string, std::string> &row);
};

#endif // CSVWRITER_H
