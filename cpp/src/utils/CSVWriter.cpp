#include "CSVWriter.h"
#include <sstream>
#include <iostream>

CSVWriter::CSVWriter(const std::string &file_path)
    : file_path(file_path), initialized(false) {}

CSVWriter::~CSVWriter() {
    flush();
    if (out_stream.is_open()) {
        out_stream.close();
    }
}

void CSVWriter::initialize() {
    std::lock_guard<std::mutex> lock(write_mutex);
    out_stream.open(file_path, std::ios::app);
    if (!out_stream.is_open()) {
        throw std::ios_base::failure("Failed to open CSV file: " + file_path);
    }
    initialized = true;
}

void CSVWriter::write_headers(const std::map<std::string, std::string> &row) {
    if (!initialized) {
        throw std::runtime_error("CSVWriter is not initialized.");
    }
    for (const auto &pair : row) {
        headers.push_back(pair.first);
        out_stream << pair.first << ",";
    }
    out_stream.seekp(-1, std::ios::cur); // Remove the trailing comma
    out_stream << "\n";
}

void CSVWriter::write_row(const std::map<std::string, std::string> &row) {
    std::lock_guard<std::mutex> lock(write_mutex);
    if (!out_stream.is_open()) {
        throw std::ios_base::failure("CSV file is not open.");
    }

    if (headers.empty()) {
        write_headers(row);
    }

    for (const auto &header : headers) {
        auto it = row.find(header);
        if (it != row.end()) {
            out_stream << it->second << ",";
        } else {
            out_stream << ",";
        }
    }
    out_stream.seekp(-1, std::ios::cur); // Remove the trailing comma
    out_stream << "\n";
}

void CSVWriter::flush() {
    std::lock_guard<std::mutex> lock(write_mutex);
    if (out_stream.is_open()) {
        out_stream.flush();
    }
}
