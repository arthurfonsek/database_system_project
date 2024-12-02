#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <omp.h>
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

// Separate columns into chunks with OpenMP parallelization
void separateColumnsToChunksWithBuffer(const std::string &inputFile, int bufferSize) {
    std::ifstream inFile(inputFile);
    std::vector<std::ofstream> columnFiles(16);

    // Open files sequentially
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
            // Parallelize writing columns
            #pragma omp parallel for
            for (int i = 0; i < 16; ++i) {
                for (const auto &item : buffer) {
                    switch (i) {
                        case 0: columnFiles[i] << item.l_orderkey << "\n"; break;
                        case 1: columnFiles[i] << item.l_partkey << "\n"; break;
                        case 2: columnFiles[i] << item.l_suppkey << "\n"; break;
                        case 3: columnFiles[i] << item.l_linenumber << "\n"; break;
                        case 4: columnFiles[i] << item.l_quantity << "\n"; break;
                        case 5: columnFiles[i] << item.l_extendedprice << "\n"; break;
                        case 6: columnFiles[i] << item.l_discount << "\n"; break;
                        case 7: columnFiles[i] << item.l_tax << "\n"; break;
                        case 8: columnFiles[i] << item.l_returnflag << "\n"; break;
                        case 9: columnFiles[i] << item.l_linestatus << "\n"; break;
                        case 10: columnFiles[i] << item.l_shipDATE << "\n"; break;
                        case 11: columnFiles[i] << item.l_commitDATE << "\n"; break;
                        case 12: columnFiles[i] << item.l_receiptDATE << "\n"; break;
                        case 13: columnFiles[i] << item.l_shipinstruct << "\n"; break;
                        case 14: columnFiles[i] << item.l_shipmode << "\n"; break;
                        case 15: columnFiles[i] << item.l_comment << "\n"; break;
                    }
                }
            }
            buffer.clear();
            rowCount = 0;
        }
    }

    // Flush remaining rows
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

// Sort a selected column chunk with OpenMP parallel sorting
void sortSelectedColumnChunkWithMemory(const std::string &inputFile, const std::string &outputFile, int memorySize) {
    std::ifstream inFile(inputFile);
    std::vector<std::string> buffer;
    std::string value;
    int rowCount = 0;

    while (std::getline(inFile, value)) {
        buffer.push_back(value);
        rowCount++;

        if (rowCount == memorySize / sizeof(std::string)) {
            #pragma omp parallel
            {
                #pragma omp single nowait
                std::sort(buffer.begin(), buffer.end());
            }

            std::ofstream outFile(outputFile, std::ios::app);
            for (const auto &val : buffer) {
                outFile << val << "\n";
            }
            outFile.close();
            buffer.clear();
            rowCount = 0;
        }
    }

    #pragma omp parallel
    {
        #pragma omp single nowait
        std::sort(buffer.begin(), buffer.end());
    }

    std::ofstream outFile(outputFile, std::ios::app);
    for (const auto &val : buffer) {
        outFile << val << "\n";
    }
    outFile.close();
    inFile.close();
}

void mergeChunksWithSortedColumn(const std::string &sortedColumnFile,
                                  const std::vector<std::string> &columnFiles,
                                  int sortedColumnIndex,
                                  const std::string &outputFile) {
    std::ifstream sortedFile(sortedColumnFile); // Coluna ordenada
    std::vector<std::ifstream> columnStreams(columnFiles.size()); // Demais colunas

    // Abrir todos os arquivos de coluna
    for (size_t i = 0; i < columnFiles.size(); ++i) {
        if (i == static_cast<size_t>(sortedColumnIndex)) {
            continue; // Não abrir o arquivo da coluna ordenada
        }
        columnStreams[i].open(columnFiles[i]);
        if (!columnStreams[i].is_open()) {
            std::cerr << "Erro ao abrir o arquivo de coluna: " << columnFiles[i] << std::endl;
            return;
        }
    }

    std::ofstream outFile(outputFile);
    if (!outFile.is_open()) {
        std::cerr << "Erro ao abrir o arquivo de saída: " << outputFile << std::endl;
        return;
    }

    std::string sortedValue;
    std::vector<std::string> columnValues(columnFiles.size());

    // Ler linha por linha
    while (std::getline(sortedFile, sortedValue)) {
        // Substituir a coluna escolhida pela ordenada
        columnValues[sortedColumnIndex] = sortedValue;

        // Ler as demais colunas
        for (size_t i = 0; i < columnFiles.size(); ++i) {
            if (i == static_cast<size_t>(sortedColumnIndex)) {
                continue; // Ignorar a coluna já ordenada
            }
            std::getline(columnStreams[i], columnValues[i]);
        }

        // Escrever a linha combinada no arquivo de saída
        for (size_t i = 0; i < columnValues.size(); ++i) {
            outFile << columnValues[i];
            if (i < columnValues.size() - 1) {
                outFile << "|";
            }
        }
        outFile << "\n";
    }

    // Fechar todos os arquivos
    sortedFile.close();
    for (auto &stream : columnStreams) {
        if (stream.is_open()) {
            stream.close();
        }
    }
    outFile.close();
}

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

    // Separar as colunas em arquivos de chunks
    separateColumnsToChunksWithBuffer("TPC-H/dbgen/lineitem.tbl", B);

    // Ordenar a coluna escolhida
    std::string selectedColumnFile = "chunk_col" + std::to_string(column + 1) + ".tbl";
    std::string sortedColumnFile = "chunk_col" + std::to_string(column + 1) + "_sorted.tbl";
    sortSelectedColumnChunkWithMemory(selectedColumnFile, sortedColumnFile, M);

    // Mesclar todas as colunas em uma tabela final com a coluna ordenada
    std::vector<std::string> columnFiles = {
        "chunk_col1.tbl", "chunk_col2.tbl", "chunk_col3.tbl", "chunk_col4.tbl",
        "chunk_col5.tbl", "chunk_col6.tbl", "chunk_col7.tbl", "chunk_col8.tbl",
        "chunk_col9.tbl", "chunk_col10.tbl", "chunk_col11.tbl", "chunk_col12.tbl",
        "chunk_col13.tbl", "chunk_col14.tbl", "chunk_col15.tbl", "chunk_col16.tbl"
    };
    mergeChunksWithSortedColumn(sortedColumnFile, columnFiles, column, "lineitem_sorted_OMP.tbl");

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;

    std::cout << "Sorting by column " << column << " completed successfully.\n";
    std::cout << "Elapsed time: " << elapsed.count() << " seconds." << std::endl;

    return 0;
}
