# DATABASE SYSTEM PROJECT

Arthur Boschini da Fonseca - 20248033

## Introduction

Using TPC-H, this project aims to work with large datasets by performing join operations and implementing external sorting to handle data efficiently within memory constraints.

## WARNING

Please make sure that you have the TPC-H files downloaded and all set up in your computer.
**THIS PROJECT IS BASED ON TCP-H TABLES**

Download link: https://www.tpc.org/

The step-by-step instruction to install properly click [here](References/statement.pdf) ---> fourth slide of the project statement.

### First part:

For the first part, the main objective was joining two tables into one:

- PART
- PARTSUPP

For this, a join operation was used, generating the final file `join_results.tbl`.

Before compiling, please make sure that the table files (`part.tbl` and `partsupp.tbl`) are in the correct path as specified in the source code. You can modify the path at lines 82 and 83.

#### To compile this part, run:

```sh
# Compile and run the first part
$ g++ -fopenmp -o first_part project_1stpart.cpp
$ ./first_part
```

### Second part:

For the second part, the main objective was to split the `lineitem.tbl` table into chunks and sort it by the column specified by the user.

The program should also be able to respect the buffer and memory size specified by the user.

The External Sort technique was used.
The program converts the `.tbl` file into `.csv` format, creating the `lineitem_fixed.csv` file, splits this large dataset into chunk files (`chunk_x.csv`), sorts them, and then merges them into one output file: `lineitem_sorted.csv`.

Main points for ensuring proper functionality:

- **RESPECT THE PROGRAM RESTRICTIONS**

When setting the buffer and memory size, please respect the program's restrictions. These restrictions are based on the available resources of *each computer*. **IF THE PROGRAM DOES NOT WORK PROPERLY, REDUCE THE MEMORY AND BUFFER SIZE**. 

#### To compile this part, run:

Before compiling, please make sure that the table file (`lineitem.tbl`) is in the correct path as specified in the source code. You can modify the path at line 240.

```sh
# Compile and run the second part
$ g++ -fopenmp -o second_part project_2ndpart.cpp
$ ./second_part
```

### PLUS

## OMP

OpenMP (OMP) was used to parallelize the join operations in `project_1stpart.cpp` and the sorting and merging operations in `project_2ndpart.cpp`. By using OpenMP, we were able to utilize multiple threads to significantly speed up the computations involved in handling large datasets. The use of OpenMP directives, such as `#pragma omp parallel for`, enabled efficient use of available CPU cores during processing.

### Changes in each file:

#### project_1stpart.cpp

- Added `#include <omp.h>` to include the OpenMP library.
- Used `#pragma omp parallel for` to parallelize the outer loop of the nested loop join to improve performance.
- Added a local `std::ostringstream` for each thread to avoid race conditions during the join operation.


#### project_2ndpart.cpp

- Added `#include <omp.h>` to include the OpenMP library.
- Used `#pragma omp parallel` and `#pragma omp for` to parallelize the sorting and merging operations.
- Added critical sections where necessary to prevent race conditions when writing to output files.

### How to compile and run

1. For both parts of the project, use the `g++` compiler with the `-fopenmp` flag to enable OpenMP support.
2. Run the compiled executable to perform the join or sorting operations.

**Example:**

```sh
# Compile and run the first part
$ g++ -fopenmp -o first_part project_1stpart.cpp
$ ./first_part

# Compile and run the second part
$ g++ -fopenmp -o second_part project_2ndpart.cpp
$ ./second_part
```

Ensure that the required `.tbl` files are in the correct paths before running the executables.
