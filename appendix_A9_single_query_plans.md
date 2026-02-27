## Appendix A9 – Single-query physical plans and distributed cut comparison

This appendix focuses on a representative query (STACK dataset, query ID 28027) and compares:

- **Distributed runtimes** under the **critical-path model** (ours) vs the **sum model**.
- **Physical plans** for this query on both engines and for both distributed cut variants.

---

### Runtime comparison

| Dataset | Query ID | DuckDB (s) | DataFusion (s) | Distributed (ours) (s) | Distributed (sum) (s) |
|---------|----------|------------|----------------|-------------------------|------------------------|
| so      | 28027    | 157.43     | 220.82         | 147.67                  | 788.02                 |

---

### Physical plans

Physical plans for this query in both engines (DuckDB and DataFusion) and distributed (critical-path vs sum objective) are available under:

[`resources/sum_model_comparison/`](resources/sum_comparison/)