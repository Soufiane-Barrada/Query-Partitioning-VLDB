## Appendix A2 – LLM assisted partitioning


The LLM receives a two-message chat prompt. The *system message* provides persistent context: the task specification (propose a semantics-preserving split that reduces latency), the full benchmark schema in DDL form, and summary table-level statistics (row counts, per-column data types, distinct counts, min/max values, null fractions). It also states the required output format and engine-specific dialect constraints (for example, unsupported SQL constructs) to maximize the fraction of executable proposals. The *user message* contains the original SQL query `Q`.

The model is instructed to return a JSON object of the form:

```json
{
  "has_cut": bool,
  "q1_engine": "duckdb" | "datafusion",
  "q2_engine": "duckdb" | "datafusion",
  "sql1": string,
  "sql2": string
}
```

When `has_cut` is `true`, `Q1` (`sql1`) is executed on `q1_engine` and its result is materialized as an Arrow table, which is then registered as an intermediate relation on `q2_engine` before executing `Q2` (`sql2`), following the same protocol used for cost-based plans.

For a concrete example of a full instantiated prompt (including schema and statistics), see:  
[`resources/LLM_input/full_tpch_prompt.txt`](resources/LLM_input/full_tpch_prompt.txt).

