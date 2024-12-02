#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <chrono>

struct LineItem {
    int l_orderkey;
    int l_partkey;
    int l_suppkey;
    int l_linenumber;
    double l_quantity;
    double l_extendedprice;
    double l_discount;
    double l_tax;
    char l_returnflag;
    char l_linestatus;
    std::string l_shipDATE;
    std::string l_commitDATE;
    std::string l_receiptDATE;
    std::string l_shipinstruct;
    std::string l_shipmode;
    std::string l_comment;
};

// Utility: Split a string by a delimiter
std::vector<std::string> split(const std::string &str, char delimiter) {
    std::vector<std::string> tokens;
    size_t start = 0, end;
    while ((end = str.find(delimiter, start)) != std::string::npos) {
        tokens.push_back(str.substr(start, end - start));
        start = end + 1;
    }
    tokens.push_back(str.substr(start));
    return tokens;
}

// Parse a line into a LineItem structure
LineItem parseLineItem(const std::string &line) {
    LineItem item;
    std::vector<std::string> columns = split(line, '|');
    item.l_orderkey = std::stoi(columns[0]);
    item.l_partkey = std::stoi(columns[1]);
    item.l_suppkey = std::stoi(columns[2]);
    item.l_linenumber = std::stoi(columns[3]);
    item.l_quantity = std::stod(columns[4]);
    item.l_extendedprice = std::stod(columns[5]);
    item.l_discount = std::stod(columns[6]);
    item.l_tax = std::stod(columns[7]);
    item.l_returnflag = columns[8][0];
    item.l_linestatus = columns[9][0];
    item.l_shipDATE = columns[10];
    item.l_commitDATE = columns[11];
    item.l_receiptDATE = columns[12];
    item.l_shipinstruct = columns[13];
    item.l_shipmode = columns[14];
    item.l_comment = columns[15];
    return item;
}

// Separate columns into chunks, respecting buffer size
void separateColumnsToChunksWithBuffer(const std::string &inputFile, int bufferSize) {
    std::ifstream inFile(inputFile);
    std::vector<std::ofstream> columnFiles(16);
    for (int i = 0; i < 16; ++i) {
        columnFiles[i].open("chunk_col" + std::to_string(i + 1) + ".tbl");
        if (!columnFiles[i].is_open()) {
            std::cerr << "Error opening file for column " << i + 1 << std::endl;
            return;
        }
    }

    std::string line;
    int rowCount = 0;
    std::vector<LineItem> buffer;

    while (std::getline(inFile, line)) {
        if (!line.empty() && line.back() == '|') line.pop_back();
        buffer.push_back(parseLineItem(line));
        rowCount++;

        if (rowCount == bufferSize / sizeof(LineItem)) {
            for (const auto &item : buffer) {
                columnFiles[0] << item.l_orderkey << "\n";
                columnFiles[1] << item.l_partkey << "\n";
                columnFiles[2] << item.l_suppkey << "\n";
                columnFiles[3] << item.l_linenumber << "\n";
                columnFiles[4] << item.l_quantity << "\n";
                columnFiles[5] << item.l_extendedprice << "\n";
                columnFiles[6] << item.l_discount << "\n";
                columnFiles[7] << item.l_tax << "\n";
                columnFiles[8] << item.l_returnflag << "\n";
                columnFiles[9] << item.l_linestatus << "\n";
                columnFiles[10] << item.l_shipDATE << "\n";
                columnFiles[11] << item.l_commitDATE << "\n";
                columnFiles[12] << item.l_receiptDATE << "\n";
                columnFiles[13] << item.l_shipinstruct << "\n";
                columnFiles[14] << item.l_shipmode << "\n";
                columnFiles[15] << item.l_comment << "\n";
            }
            buffer.clear();
            rowCount = 0;
        }
    }

    for (const auto &item : buffer) {
        columnFiles[0] << item.l_orderkey << "\n";
        columnFiles[1] << item.l_partkey << "\n";
        columnFiles[2] << item.l_suppkey << "\n";
        columnFiles[3] << item.l_linenumber << "\n";
        columnFiles[4] << item.l_quantity << "\n";
        columnFiles[5] << item.l_extendedprice << "\n";
        columnFiles[6] << item.l_discount << "\n";
        columnFiles[7] << item.l_tax << "\n";
        columnFiles[8] << item.l_returnflag << "\n";
        columnFiles[9] << item.l_linestatus << "\n";
        columnFiles[10] << item.l_shipDATE << "\n";
        columnFiles[11] << item.l_commitDATE << "\n";
        columnFiles[12] << item.l_receiptDATE << "\n";
        columnFiles[13] << item.l_shipinstruct << "\n";
        columnFiles[14] << item.l_shipmode << "\n";
        columnFiles[15] << item.l_comment << "\n";
    }

    for (auto &file : columnFiles) {
        file.close();
    }
    inFile.close();
}

// Sort a selected column chunk, respecting memory size
void sortSelectedColumnChunkWithMemory(const std::string &inputFile, const std::string &outputFile, int memorySize) {
    std::ifstream inFile(inputFile);
    std::vector<std::string> buffer;
    std::string value;
    int rowCount = 0;

    while (std::getline(inFile, value)) {
        buffer.push_back(value);
        rowCount++;

        if (rowCount == memorySize / sizeof(std::string)) {
            std::sort(buffer.begin(), buffer.end());
            std::ofstream outFile(outputFile, std::ios::app);
            for (const auto &val : buffer) {
                outFile << val << "\n";
            }
            outFile.close();
            buffer.clear();
            rowCount = 0;
        }
    }

    std::sort(buffer.begin(), buffer.end());
    std::ofstream outFile(outputFile, std::ios::app);
    for (const auto &val : buffer) {
        outFile << val << "\n";
    }
    outFile.close();
    inFile.close();
}

// Merge columns based on sorted column
void mergeColumnsWithSortedColumn(const std::string &sortedColumnFile,
                                  const std::vector<std::string> &columnFiles,
                                  int sortedColumnIndex,
                                  const std::string &outputFile) {
    // Open the sorted column file
    std::ifstream sortedFile(sortedColumnFile);
    if (!sortedFile.is_open()) {
        std::cerr << "Error opening sorted column file: " << sortedColumnFile << std::endl;
        return;
    }

    // Open all column files
    std::vector<std::ifstream> columnStreams(columnFiles.size());
    for (size_t i = 0; i < columnFiles.size(); ++i) {
        columnStreams[i].open(columnFiles[i]);
        if (!columnStreams[i].is_open()) {
            std::cerr << "Error opening column file: " << columnFiles[i] << std::endl;
            return;
        }
    }

    // Open the output file
    std::ofstream outFile(outputFile);
    if (!outFile.is_open()) {
        std::cerr << "Error opening output file: " << outputFile << std::endl;
        return;
    }

    std::string sortedValue;
    std::vector<std::string> columnValues(columnFiles.size());

    // Process each line in the sorted column file
    while (std::getline(sortedFile, sortedValue)) {
        for (size_t i = 0; i < columnFiles.size(); ++i) {
            if (i == static_cast<size_t>(sortedColumnIndex)) {
                columnValues[i] = sortedValue; // Use the sorted column value
            } else {
                std::getline(columnStreams[i], columnValues[i]); // Use the original values for other columns
            }
        }

        // Write the reconstructed row to the output file
        for (size_t i = 0; i < columnValues.size(); ++i) {
            outFile << columnValues[i];
            if (i < columnValues.size() - 1) {
                outFile << "|";
            }
        }
        outFile << "\n";
    }

    // Close all files
    sortedFile.close();
    for (auto &stream : columnStreams) {
        stream.close();
    }
    outFile.close();
}


// Main Function
int main() {
    int B_MB, M_GB, column;
    std::cout << "Enter the size of the buffer [MB] (MAXIMUM 200): ";
    std::cin >> B_MB;
    std::cout << "Enter the size of the memory [MB]  (MAXIMUM 1024 (1GB)): ";
    std::cin >> M_GB;
    std::cout << "Enter the column to sort by (0 to 15): ";
    std::cin >> column;

    if (column < 0 || column >= 16) {
        std::cerr << "Invalid column index!" << std::endl;
        return 1;
    }
    if (M_GB > 1024 || M_GB < 0 || B_MB > 200 || B_MB < 0 || B_MB > M_GB) {
        std::cerr << "Invalid buffer or memory size!" << std::endl;
        return 1;
    }

    int B = B_MB * 1024 * 1024;
    int M = M_GB * 1024 * 1024;

    auto start = std::chrono::high_resolution_clock::now();
    separateColumnsToChunksWithBuffer("TPC-H/dbgen/lineitem.tbl", B);

    // Sort the selected column
    std::string selectedColumnFile = "chunk_col" + std::to_string(column + 1) + ".tbl";
    std::string sortedColumnFile = "chunk_col" + std::to_string(column + 1) + "_sorted.tbl";
    sortSelectedColumnChunkWithMemory(selectedColumnFile, sortedColumnFile, M);

    // Merge all columns based on the sorted column
    std::vector<std::string> columnFiles = {
        "chunk_col1.tbl", "chunk_col2.tbl", "chunk_col3.tbl", "chunk_col4.tbl",
        "chunk_col5.tbl", "chunk_col6.tbl", "chunk_col7.tbl", "chunk_col8.tbl",
        "chunk_col9.tbl", "chunk_col10.tbl", "chunk_col11.tbl", "chunk_col12.tbl",
        "chunk_col13.tbl", "chunk_col14.tbl", "chunk_col15.tbl", "chunk_col16.tbl"
    };
    mergeColumnsWithSortedColumn(sortedColumnFile, columnFiles, column, "lineitem_sorted_foi.tbl");

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;

    std::cout << "Sorting by column " << column << " completed successfully.\n";
    std::cout << "Elapsed time: " << elapsed.count() << " seconds." << std::endl;

    return 0;
}