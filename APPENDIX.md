## Appendix overview

This document defines the **GitHub appendix** for the paper:

**Splitting Queries Across Engines: A Critical-Path Approach**  

---

### Appendix A1 – Placement Algorithm

  - [`appendix_A1_placement_algorithm.md`](appendix_A1_placement_algorithm.md) – full pseudocode for the critical‑path DP.

---

### Appendix A2 – LLM-assisted partitioning prompt

- [`appendix_A2_llm_prompt.md`](appendix_A2_llm_prompt.md): detailed description of the system and user messages, required JSON schema, and execution protocol.
  

---

### Appendix A3 – Transfer cost model coefficients
  - [`appendix_A3_transfer_coefficients.md`](appendix_A3_transfer_coefficients.md): table of the learned coefficients (including RMSE and R^2).

---

### Appendix A4 – Physical plans and top-10 winners

  - [`appendix_A4_physical_plans_top10.md`](appendix_A4_physical_plans_top10.md): brief description of these artifacts and how they map to the paper.

---

### Appendix A5 – Operator LCM hyperparameters

  - [`appendix_A5_operator_hyperparams.md`](appendix_A5_operator_hyperparams.md): LaTeX table of selected hyperparameters (model type, number of estimators, depth, learning rate, regularisation) for all 8 (engine, operator) pairs.

---

### Appendix A6 – Per-dataset partitioning results tables

  - [`appendix_A6_partioning.md`](appendix_A6_partioning.md): Partitioning results tables.

---

### Appendix A7 – Query runtime percentile buckets

  - [`appendix_A7_runtime_percentiles.md`](appendix_A7_runtime_percentiles.md): LaTeX table of min/max times and query counts per bucket.

---

### Appendix A8 – Cut plans

  - [`appendix_A8_cut_plans.md`](appendix_A8_cut_plans.md): cut plans used in the evaluation.

---

### Appendix A9 – Single-query physical plans and cut comparison

  - [`appendix_A9_single_query_plans.md`](appendix_A9_single_query_plans.md): single-query physical plans on both engines, and a comparison between critical-path and sum-objective distributed cuts.
