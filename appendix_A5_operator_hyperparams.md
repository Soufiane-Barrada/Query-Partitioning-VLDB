## Appendix A5 – Operator-level learned cost model hyperparameters

Below is the table of the selected hyperparameters for the operator models used in the paper.

| Engine    | Operator | Model | N_est | Depth | Learning rate (η) | Regularization (λ) |
|----------|----------|-------|-------|-------|--------------------|--------------------|
| DuckDB   | Sort     | LGBM  | 1,308 | 13    | 0.196              | 1.17               |
| DuckDB   | Join     | XGB   | 5,779 | 12    | 0.007              | 2.94               |
| DuckDB   | Filter   | XGB   | 9,615 | 3     | 0.008              | 0.64               |
| DuckDB   | Agg      | XGB   | 1,185 | 5     | 0.001              | 0.31               |
| DataFusion | Sort   | XGB   | 2,422 | 9     | 0.051              | 2.33               |
| DataFusion | Join   | XGB   | 1,246 | 12    | 0.052              | 0.36               |
| DataFusion | Filter | XGB   | 892   | 12    | 0.026              | 2.94               |
| DataFusion | Agg    | XGB   | 8,304 | 11    | 0.054              | 2.10               |

The trained model artefacts and their Bayesian hyperparameter search logs are stored under [`resources/runs_operator_models/kept/`](resources/runs_operator_models/kept/), organised by engine and operator type:

```
resources/runs_operator_models/kept/
├── duckdb/
│   ├── sorts/
│   ├── joins/
│   ├── filters/
│   └── aggregates/
└── datafusion/
    ├── sorts/
    ├── joins/
    ├── filters/
    └── aggregates/
```
