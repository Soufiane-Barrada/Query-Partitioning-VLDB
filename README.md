## Splitting Queries Across Engines: Artifact / Appendix Repo

This repository contains the code and experimental artifacts for the paper **“Splitting Queries Across Engines: A Critical-Path Approach”**.  

### Current appendix mapping

- **Appendix A1 – Query splitting and cost-based partitioning**
  - Full pseudocode for the placement algorithm is in [`appendix_A1_placement_algorithm.md`](appendix_A1_placement_algorithm.md).
  
- **Appendix A2 – LLM-assisted partitioning prompt**
  - Describes the two-message chat prompt (system + user), required JSON output schema, and execution protocol for the LLM baseline.
  - Full description is in [`appendix_A2_llm_prompt.md`](appendix_A2_llm_prompt.md).
  
- **Appendix A3 – Transfer cost model coefficients**
  - Contains the table of learned Arrow transfer cost coefficients used by the linear transfer model in Section 4.4.
  - Full table is in [`appendix_A3_transfer_coefficients.md`](appendix_A3_transfer_coefficients.md).
  
- **Appendix A4 – Physical plans and top-10 winners**
  - Contains physical execution plans and times for the 20 distributed wins (10 DuckDB-faster, 10 DataFusion-faster).
  - Overview is in [`appendix_A4_physical_plans_top10.md`](appendix_A4_physical_plans_top10.md).

- **Appendix A5 – Operator models hyperparameters**
  - Full table is in [`appendix_A5_operator_hyperparams.md`](appendix_A5_operator_hyperparams.md).

- **Appendix A6 – Per-dataset partitioning results tables**
  - Contains the full JOB, STACK, and TPC-H partitioning result tables (strict/soft wins, gains, and speedups) for all variants and baselines.
  - Tables are in [`appendix_A6_partioning.md`](appendix_A6_partioning.md).

- **Appendix A7 – Query runtime percentile buckets**
  - Contains the four equal-sized runtime buckets (p0–p25 … p75–p100), used to stratify win-rate analysis.
  - Tables are in [`appendix_A7_runtime_percentiles.md`](appendix_A7_runtime_percentiles.md).

- **Appendix A8 – Cut plans**
  - Calcite cut plans for all variants and LLM splits.
  - Overview is in [`appendix_A8_cut_plans.md`](appendix_A8_cut_plans.md).

- **Appendix A9 – Single-query physical plans and cut comparison**
  - Shows the physical plans for a representative query on both engines and compares the distributed cuts chosen by the critical-path objective and the sum-objective baseline.
  - Details are in [`appendix_A9_single_query_plans.md`](appendix_A9_single_query_plans.md).
