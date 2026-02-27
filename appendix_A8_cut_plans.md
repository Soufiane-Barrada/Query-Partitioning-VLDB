## Appendix A8 – Cut plan

This appendix points to the JSON cut plans used in the evaluation: Calcite-based plans for all variants and the LLM query splits.

---

### Cut plans (Calcite output and LLM splits)

All cut plans are stored under [`resources/cut_plans/`](resources/cut_plans/):

```
resources/cut_plans/
├── job_calcite_withOptim_withTransfer_withData_testOnly/
├── job_calcite_withOptim_noTransfer_withData_testOnly/
├── job_calcite_noOptim_withTransfer_withData_testOnly/
├── job_calcite_noOptim_noTransfer_withData_testOnly/
├── job_calcite_random_testOnly/
├── so_calcite_withOptim_withTransfer_withData_testOnly/
├── so_calcite_withOptim_noTransfer_withData_testOnly/
├── so_calcite_noOptim_withTransfer_withData_testOnly/
├── so_calcite_noOptim_noTransfer_withData_testOnly/
├── so_calcite_random_testOnly/
├── tpch_calcite_withOptim_withTransfer_withData_testOnly/
├── tpch_calcite_withOptim_noTransfer_withData_testOnly/
├── tpch_calcite_noOptim_withTransfer_withData_testOnly/
├── tpch_calcite_noOptim_noTransfer_withData_testOnly/
├── tpch_calcite_random_testOnly/
├── job_llm_query_splits.jsonl
├── so_llm_query_splits.jsonl
├── tpch_llm_query_splits.jsonl
└── query_status_summary.md
```

**Calcite directory naming**: `{dataset}_calcite_{optim}_{transfer}_withData_testOnly`, where:
- `withOptim` / `noOptim` – whether Calcite logical rewrites are applied before cut selection.
- `withTransfer` / `noTransfer` – whether transfer costs are included in the DP objective.
- `random` – random cut placement baseline.

**LLM splits**: One JSONL file per dataset (`{dataset}_llm_query_splits.jsonl`), each line containing the LLM’s JSON proposal (has_cut, q1_engine, q2_engine, sql1, sql2) for the corresponding query.
