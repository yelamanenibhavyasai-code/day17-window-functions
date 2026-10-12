# Day 17 — Spark SQL Window Functions

## Project Overview

This project demonstrates window functions using Scala and Apache Spark SQL. It covers student rankings, latest insurance policy selection, and previous/next record analysis.

## Technologies Used

- Scala 2.12.18
- Apache Spark 3.5.3
- Java 17
- SBT 1.12.15
- Ubuntu on WSL2

## Concepts Covered

- `row_number()` — Assigns a unique sequential number to each row.
- `rank()` — Assigns ranks and leaves gaps after ties.
- `dense_rank()` — Assigns ranks without gaps after ties.
- `partitionBy()` — Divides rows into groups for window calculations.
- `lag()` — Retrieves a previous row's value.
- `lead()` — Retrieves a following row's value.

## Scenarios Implemented

### 1. Top 3 Students per Course

Ranks students by marks within each course and selects the top three students.

### 2. Latest Policy per Customer

Uses customer-based partitioning and descending policy dates to select the latest policy for each customer.

### 3. Previous and Next Trip Fare

Uses `lag()` and `lead()` to compare fares across trips for each driver and route.

## Project Structure

```text
day17-window-functions/
├── build.sbt
├── project/
│   └── build.properties
├── src/
│   └── main/
│       └── scala/
│           └── Day17WindowFunctions.scala
└── README.md
```

## How to Run

Compile the project:

```bash
sbt compile
```

Run the application:

```bash
sbt "runMain Day17WindowFunctions"
```

## Key Learnings

- Ranking records using Spark window functions.
- Partitioning calculations by course and customer.
- Selecting the latest record per customer.
- Comparing previous and next records.
- Applying window functions to practical datasets.

## Author

**Yelamaneni Bhavyasai**

30 Days Scala and Apache Spark Training — Day 17.
