## Appendix A4 – Physical plans for top distributed wins

This appendix lists the **20 distributed wins** discussed in the paper: 10 queries where DuckDB is the faster single-engine baseline and 10 where DataFusion is faster. For each query, the physical plan dumps are stored in [`resources/physical_plans_top10/`](resources/physical_plans_top10/).

All times are in **seconds**.

---

### Table 1 – Top 10 distributed wins where DuckDB is the faster single-engine baseline

| Dataset | Query ID | DuckDB (s) | DataFusion (s) | Distributed (s) | Q1 Engine | Q2 Engine |
|---------|----------|------------|----------------|-----------------|-----------|-----------|
| JOB     | 6283     | 0.528      | 1713.557       | 0.479           | duckdb    | datafusion |
| JOB     | 14248    | 23.112     | 1521.945       | 3.006           | datafusion | duckdb    |
| JOB     | 26114    | 0.779      | 741.432        | 0.666           | duckdb    | datafusion |
| STACK   | 12360    | 346.951    | 902.395        | 216.095         | datafusion | duckdb    |
| STACK   | 12246    | 0.939      | 281.008        | 0.662           | duckdb    | datafusion |
| JOB     | 11280    | 6.005      | 250.545        | 5.559           | duckdb    | datafusion |
| TPC-H   | 14460    | 2.193      | 216.779        | 1.572           | duckdb    | datafusion |
| TPC-H   | 14474    | 25.932     | 201.657        | 10.820          | duckdb    | datafusion |
| TPC-H   | 9188     | 3.339      | 179.054        | 0.986           | duckdb    | datafusion |
| TPC-H   | 28561    | 0.719      | 140.215        | 0.671           | duckdb    | datafusion |

Physical plans: [`resources/physical_plans_top10/duckdb_faster/`](resources/physical_plans_top10/duckdb_faster/)

---

### Table 2 – Top 10 distributed wins where DataFusion is the faster single-engine baseline

| Dataset | Query ID | DuckDB (s) | DataFusion (s) | Distributed (s) | Q1 Engine  | Q2 Engine  |
|---------|----------|------------|----------------|-----------------|------------|------------|
| TPC-H   | 7199     | 2335.341   | 6.673          | 5.469           | duckdb     | datafusion |
| STACK   | 19637    | 505.442    | 4.544          | 0.655           | duckdb     | datafusion |
| STACK   | 11507    | 60.280     | 17.714         | 11.935          | datafusion | duckdb     |
| STACK   | 14891    | 52.613     | 11.362         | 4.524           | datafusion | duckdb     |
| TPC-H   | 11254    | 29.131     | 12.942         | 5.106           | duckdb     | datafusion |
| STACK   | 5315     | 21.179     | 7.714          | 4.502           | duckdb     | datafusion |
| STACK   | 31598    | 18.166     | 7.103          | 5.028           | duckdb     | datafusion |
| TPC-H   | 10773    | 32.577     | 25.414         | 6.390           | duckdb     | datafusion |
| STACK   | 13160    | 16.360     | 9.670          | 4.771           | duckdb     | datafusion |
| STACK   | 11477    | 16.409     | 9.754          | 5.786           | duckdb     | datafusion |

Physical plans: [`resources/physical_plans_top10/datafusion_faster/`](resources/physical_plans_top10/datafusion_faster/)
