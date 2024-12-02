#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <omp.h>

// Structure to hold column data for 'part'
struct Part {
    int p_partkey;
    std::string p_name;
    std::string p_mfgr;
    std::string p_brand;
    std::string p_type;
    int p_size;
    std::string p_container;
    double p_retailprice;
    std::string p_comment;
};

// Structure to hold column data for 'partsupp'
struct PartSupp {
    int ps_partkey;
    int ps_suppkey;
    int ps_availqty;
    double ps_supplycost;
    std::string ps_comment;
};

// Helper function to split a line into fields
std::vector<std::string> splitLine(const std::string &line, char delimiter) {
    std::vector<std::string> fields;
    std::istringstream ss(line);
    std::string field;
    while (std::getline(ss, field, delimiter)) {
        fields.push_back(field);
    }
    return fields;
}

// Load 'part' table data into a map
std::unordered_map<int, Part> loadPartTable(const std::string &filePath) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
        std::cerr << "Error opening PART file." << std::endl;
        exit(1);
    }

    std::unordered_map<int, Part> partMap;
    std::string line;
    while (std::getline(file, line)) {
        auto fields = splitLine(line, '|');
        if (fields.size() != 9) {
            std::cerr << "Malformed PART row: " << line << std::endl;
            continue;
        }

        Part part = {
            std::stoi(fields[0]),
            fields[1],
            fields[2],
            fields[3],
            fields[4],
            std::stoi(fields[5]),
            fields[6],
            std::stod(fields[7]),
            fields[8]
        };

        partMap[part.p_partkey] = part;
    }
    file.close();
    return partMap;
}

// Process 'partsupp' table and perform join with OpenMP parallelization
void processPartSupp(const std::string &partSuppFile, const std::unordered_map<int, Part> &partMap, const std::string &outputFile) {
    std::ifstream file(partSuppFile);
    if (!file.is_open()) {
        std::cerr << "Error opening PARTSUPP file." << std::endl;
        exit(1);
    }

    // Vector to hold lines for processing
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(file, line)) {
        lines.push_back(line);
    }
    file.close();

    // Use OpenMP to parallelize processing
    std::ofstream outFile(outputFile);
    if (!outFile.is_open()) {
        std::cerr << "Error opening output file." << std::endl;
        exit(1);
    }

    // Protect output file writing
    #pragma omp parallel
    {
        std::ostringstream localBuffer;

        #pragma omp for
        for (size_t i = 0; i < lines.size(); ++i) {
            auto fields = splitLine(lines[i], '|');
            if (fields.size() != 5) {
                std::cerr << "Malformed PARTSUPP row: " << lines[i] << std::endl;
                continue;
            }

            PartSupp partsupp = {
                std::stoi(fields[0]),
                std::stoi(fields[1]),
                std::stoi(fields[2]),
                std::stod(fields[3]),
                fields[4]
            };

            // Check if part exists in the map
            if (partMap.find(partsupp.ps_partkey) != partMap.end()) {
                const Part &part = partMap.at(partsupp.ps_partkey);

                // Append to local buffer
                localBuffer << part.p_partkey << "|" << part.p_name << "|" << part.p_mfgr << "|"
                            << part.p_brand << "|" << part.p_type << "|" << part.p_size << "|"
                            << part.p_container << "|" << part.p_retailprice << "|" << part.p_comment << "|"
                            << partsupp.ps_suppkey << "|" << partsupp.ps_availqty << "|"
                            << partsupp.ps_supplycost << "|" << partsupp.ps_comment << "\n";
            }
        }

        // Write from local buffer to the output file
        #pragma omp critical
        {
            outFile << localBuffer.str();
        }
    }

    outFile.close();
}

int main() {
    std::string partFilePath = "TPC-H/dbgen/part.tbl";
    std::string partSuppFilePath = "TPC-H/dbgen/partsupp.tbl";
    std::string outputFilePath = "join_results_parallel.tbl";

    auto start_time = std::chrono::high_resolution_clock::now();

    // Load part table into a hash map
    std::unordered_map<int, Part> partMap = loadPartTable(partFilePath);

    // Process partsupp and join
    processPartSupp(partSuppFilePath, partMap, outputFilePath);

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time = end_time - start_time;
    std::cout << "Elapsed time: " << elapsed_time.count() << " seconds." << std::endl;

    return 0;
}
