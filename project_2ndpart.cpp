#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <queue>
#include <chrono>

const int NUM_COLUMNS = 16;  // NUMBER OF COLUMNS IN THE TEXT

// SPLIT LINE (lineitem.tbl)
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

// CONVERTING TO CSV
void convertToCSV(const std::string &inputFile, const std::string &outputFile) {
    std::ifstream inFile(inputFile);
    std::ofstream outFile(outputFile);

    if (!inFile.is_open() || !outFile.is_open()) {
        std::cerr << "File opening error." << std::endl;
        return;
    }

    int lineCount = 0;
    std::string line;
    while (std::getline(inFile, line)) {
        if (!line.empty() && line.back() == '|') line.pop_back();
        std::vector<std::string> columns = split(line, '|');
        for (size_t i = 0; i < columns.size(); ++i) {
            outFile << columns[i];
            if (i < columns.size() - 1) {
                outFile << ",";
            }
        }
        outFile << "\n";
        lineCount++;
    }
    inFile.close();
    outFile.close();

    std::cout << "File `lineitem_fixed.csv` successfully generated with " << lineCount << " lines." << std::endl;
}

// LOAD RECORDS IN CHUNKS
std::vector<std::vector<std::string>> loadRecordsInChunks(std::ifstream &inFile, int num_records) {
    std::vector<std::vector<std::string>> buffer;
    std::string line;
    int count = 0;

    while (count < num_records && std::getline(inFile, line)) {
        if (!line.empty()) {
            buffer.push_back(split(line, ','));
            ++count;
        }
    }

    std::cout << "Loaded " << buffer.size() << " records in the current chunk." << std::endl;
    return buffer;
}

// FUNCTION TO SORT DATA BY A SPECIFIED COLUMN
void sortRecordsByColumn(std::vector<std::vector<std::string>> &records, int column) {
    
    /*
    TRANSFORM THIS FUNCTION INTO OMP
    */

    if (records.empty()) {
        std::cerr << "Error: no data to sort." << std::endl;
        return;
    }

    std::cout << "Sorting " << records.size() << " records by column " << column << std::endl;
    std::sort(records.begin(), records.end(), [column](const std::vector<std::string> &a, const std::vector<std::string> &b) {
        return a[column] < b[column];
    });

    std::cout << "All data sorted." << std::endl;
}

// FUNCTION TO MERGE SORTED CHUNKS INTO A FINAL SORTED FILE
void mergeSortedChunks(const std::vector<std::string> &chunkFiles, const std::string &outputFile, int column) {
    std::ofstream outFile(outputFile);
    if (!outFile.is_open()) {
        std::cerr << "Error opening output file for merging." << std::endl;
        return;
    }
    auto compare = [column](const std::pair<std::vector<std::string>, int> &a, const std::pair<std::vector<std::string>, int> &b) {
        return a.first[column] > b.first[column];
    };

    /*starting a queue so we can keep track of the smallest 
    element from each of the chunk files at any point, 
    allowing the merging process to maintain order efficiently
    */

    std::priority_queue<std::pair<std::vector<std::string>, int>, std::vector<std::pair<std::vector<std::string>, int>>, decltype(compare)> minHeap(compare);
    std::vector<std::ifstream> chunkStreams(chunkFiles.size());
    for (size_t i = 0; i < chunkFiles.size(); ++i) {
        chunkStreams[i].open(chunkFiles[i]);
        if (!chunkStreams[i].is_open()) {
            std::cerr << "Error opening chunk file: " << chunkFiles[i] << std::endl;
            return;
        }
        std::string line;
        if (std::getline(chunkStreams[i], line)) {
            minHeap.emplace(split(line, ','), i);
        }
    }

    while (!minHeap.empty()) {
        auto [record, index] = minHeap.top();
        minHeap.pop();
        for (size_t i = 0; i < record.size(); ++i) {
            outFile << record[i];
            if (i < record.size() - 1) {
                outFile << ",";
            }
        }
        outFile << "\n";
        //std::cout << "Written record to output file: " << record[0] << "..." << std::endl;
        // Read the next record from the same chunk file
        std::string line;
        if (std::getline(chunkStreams[index], line)) {
            minHeap.emplace(split(line, ','), index);
        }
    }
    //CLOSING
    for (auto &stream : chunkStreams) {
        stream.close();
    }
    outFile.close();
    std::cout << "Merging of sorted chunks completed." << std::endl;
}

// MAIN FUNCTION TO SORT IN CHUNKS RESPECTING THE MEMORY LIMIT
void sortCSVRecords(const std::string &inputFile, const std::string &outputFile, int column, int B_MB, int M_GB) {
    std::ifstream inFile(inputFile);

    if (!inFile.is_open()) {
        std::cerr << "Error: could not open the file to sort." << std::endl;
        return;
    }
    inFile.clear();
    inFile.seekg(0, std::ios::beg);

    //MB to bytes
    int B = B_MB * 1024 * 1024;  // Buffer size
    int M = M_GB * 1024 * 1024; // Memory limit

    int avg_record_size = 256;  
    int num_records_per_buffer = B / avg_record_size;
    int num_buffers = M / B;

    std::vector<std::vector<std::string>> buffer;
    std::vector<std::string> chunkFiles;
    int chunkIndex = 0;

    while (true) {
        buffer = loadRecordsInChunks(inFile, num_records_per_buffer * num_buffers);
        if (buffer.empty()) {
            std::cout << "No more records to load." << std::endl;
            std::cout << "Starting the merging process." << std::endl;
            break;
        }
        sortRecordsByColumn(buffer, column);
        std::string chunkFileName = "chunk_" + std::to_string(chunkIndex++) + ".csv";
        std::ofstream chunkFile(chunkFileName);
        if (!chunkFile.is_open()) {
            std::cerr << "Error opening chunk file for writing." << std::endl;
            return;
        }

        for (const auto &record : buffer) {
            for (size_t i = 0; i < record.size(); ++i) {
                chunkFile << record[i];
                if (i < record.size() - 1) {
                    chunkFile << ",";
                }
            }
            chunkFile << "\n";
            //std::cout << "Written record to chunk file: " << record[0] << "..." << std::endl;
        }
        chunkFile.close();
        chunkFiles.push_back(chunkFileName);

        std::cout << "Chunk written to file: " << chunkFileName << std::endl;
    }

    inFile.close();
    mergeSortedChunks(chunkFiles, outputFile, column);
    std::cout << "Sorting and merging completed." << std::endl;
}

int main() {
    int B_MB, M_GB, column;
    std::cout << "Enter the size of the buffer [MB] (MAXIMUM 200): ";
    std::cin >> B_MB;
    std::cout << "Enter the size of the memory [MB]  (MAXIMUM 1024 (1GB)): ";
    std::cin >> M_GB;
    std::cout << "Enter the column to sort by (0 to " << NUM_COLUMNS - 1 << "): ";
    std::cin >> column;

    if (column < 0 || column >= NUM_COLUMNS) {
        std::cerr << "Invalid column index." << std::endl;
        return 1;
    }
    if (M_GB > 1024 || M_GB < 0){
        std::cerr << "Invalid size of memory." << std::endl;
        return 1;
    }
    if (B_MB > 200 || B_MB < 0){
        std::cerr << "Invalid size of buffer." << std::endl;
        return 1;
    }
    if (B_MB > M_GB){
        std::cerr << "The buffer size can not be greater than the memory size" << std::endl;
        return 1;
    }

    //STARTING THE TIMER
    auto start_time = std::chrono::high_resolution_clock::now();

    convertToCSV("TPC-H/dbgen/lineitem.tbl", "lineitem_fixed.csv");
    sortCSVRecords("lineitem_fixed.csv", "lineitem_sorted.csv", column, B_MB, M_GB);

    //STOPPING THE TIMER
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time = end_time - start_time;
    std::cout << std::endl;
    std::cout << "Conversion, sorting, and merging completed respecting the buffer and memory limit." << std::endl;
    std::cout << "Elapsed time: " << elapsed_time.count() << " seconds." << std::endl;
    std::cout << std::endl;

    return 0;
}
