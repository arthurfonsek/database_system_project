#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <sstream>
#include <omp.h>
#include <chrono>

struct Part {
    int partkey;
    std::string otherFields;
};

struct PartSupp {
    int partkey;
    std::string otherFields;
};

std::vector<Part> loadPartTable(const std::string &filePath) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
        std::cerr << "Error opening PART file." << std::endl;
        exit(1);
    }

    std::vector<Part> partTable;
    std::string line;
    while (std::getline(file, line)) {
        std::istringstream iss(line);
        Part part;
        iss >> part.partkey;
        std::getline(iss, part.otherFields); 
        partTable.push_back(part);
    }
    file.close();
    return partTable;
}

std::vector<PartSupp> loadPartSuppTable(const std::string &filePath) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
        std::cerr << "Error opening PARTSUPP file." << std::endl;
        exit(1);
    }

    std::vector<PartSupp> partSuppTable;
    std::string line;
    while (std::getline(file, line)) {
        std::istringstream iss(line);
        PartSupp partsupp;
        iss >> partsupp.partkey;
        std::getline(iss, partsupp.otherFields); 
        partSuppTable.push_back(partsupp);
    }
    file.close();
    return partSuppTable;
}

void nestedLoopJoin(const std::vector<Part> &partTable, const std::vector<PartSupp> &partSuppTable, const std::string &outputFile) {
    std::ofstream outFile(outputFile);
    if (!outFile.is_open()) {
        std::cerr << "Error opening output file." << std::endl;
        exit(1);
    }

    #pragma omp parallel for
    for (size_t i = 0; i < partTable.size(); ++i) {
        std::ostringstream localOutput;
        for (const auto &partsupp : partSuppTable) {
            if (partTable[i].partkey == partsupp.partkey) { 
                localOutput << partTable[i].partkey << partTable[i].otherFields << partsupp.otherFields << "\n";
            }
        }

        #pragma omp critical
        {
            outFile << localOutput.str();
        }
    }

    outFile.close();
    std::cout << "Join completed and results saved to file: " << outputFile << std::endl;
}

int main() {
    std::string partFilePath = "TPC-H/dbgen/part.tbl";
    std::string partSuppFilePath = "TPC-H/dbgen/partsupp.tbl";
    std::string outputFilePath = "join_results.tbl";

    auto start_time = std::chrono::high_resolution_clock::now();

    std::vector<Part> partTable = loadPartTable(partFilePath);
    std::vector<PartSupp> partSuppTable = loadPartSuppTable(partSuppFilePath);

    nestedLoopJoin(partTable, partSuppTable, outputFilePath);

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time = end_time - start_time;
    std::cout << "Elapsed time: " << elapsed_time.count() << " seconds." << std::endl;

    return 0;
}
