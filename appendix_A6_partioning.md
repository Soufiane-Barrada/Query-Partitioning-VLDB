## Appendix A6 – Partitioning results tables

Below are the three per-dataset partitioning results tables used in the paper, written directly in markdown.

### Overall Partitioning Results on the JOB (IMDb) Dataset

| Experiment                 | Comparison        | Total Queries | Wins | Win Rate | Mean Gain | Mean Speedup | Max Speedup |
|---------------------------|-------------------|---------------|------|----------|-----------|--------------|-------------|
| Ours (opt + xfer)         | Strict (vs Best)  | 13            | 3    | 23.1%    | 46.7%     | 2.49x        | 4.43x       |
| Ours (opt + xfer)         | Soft (vs Worst)   | 13            | 13   | 100.0%   | 73.5%     | 6.20x        | 24.37x      |
| Ours (opt, no xfer)       | Strict (vs Best)  | 91            | 11   | 12.1%    | 25.3%     | 1.79x        | 4.71x       |
| Ours (opt, no xfer)       | Soft (vs Worst)   | 91            | 80   | 87.9%    | 86.2%     | 114.08x      | 2256.06x    |
| Ours (no opt, no xfer)    | Strict (vs Best)  | 65            | 15   | 23.1%    | 15.9%     | 1.43x        | 5.03x       |
| Ours (no opt, no xfer)    | Soft (vs Worst)   | 65            | 52   | 80.0%    | 81.9%     | 156.83x      | 3577.07x    |
| Ours (no opt + xfer)      | Strict (vs Best)  | 3             | 2    | 66.7%    | 40.7%     | 2.81x        | 4.58x       |
| Ours (no opt + xfer)      | Soft (vs Worst)   | 3             | 3    | 100.0%   | 86.7%     | 40.87x       | 92.26x      |
| Random baseline           | Strict (vs Best)  | 162           | 12   | 7.4%     | 40.8%     | 3.01x        | 10.80x      |
| Random baseline           | Soft (vs Worst)   | 162           | 122  | 75.3%    | 66.6%     | 42.07x       | 2372.83x    |
| LLM baseline              | Strict (vs Best)  | 147           | 8    | 5.4%     | 36.7%     | 2.00x        | 3.14x       |
| LLM baseline              | Soft (vs Worst)   | 147           | 130  | 88.4%    | 57.9%     | 11.95x       | 449.95x     |

### Overall Partitioning Results on the STACK (SO) Dataset

| Experiment                 | Comparison        | Total Queries | Wins | Win Rate | Mean Gain | Mean Speedup | Max Speedup |
|---------------------------|-------------------|---------------|------|----------|-----------|--------------|-------------|
| Ours (opt + xfer)         | Strict (vs Best)  | 33            | 0    | 0.0%     | ---       | ---          | ---         |
| Ours (opt + xfer)         | Soft (vs Worst)   | 33            | 24   | 72.7%    | 49.9%     | 16.98x       | 355.76x     |
| Ours (opt, no xfer)       | Strict (vs Best)  | 229           | 26   | 11.4%    | 9.3%      | 1.12x        | 1.66x       |
| Ours (opt, no xfer)       | Soft (vs Worst)   | 229           | 155  | 67.7%    | 58.3%     | 10.75x       | 424.43x     |
| Ours (no opt, no xfer)    | Strict (vs Best)  | 185           | 20   | 10.8%    | 9.5%      | 1.17x        | 2.93x       |
| Ours (no opt, no xfer)    | Soft (vs Worst)   | 185           | 136  | 73.5%    | 57.8%     | 4.28x        | 30.43x      |
| Ours (no opt + xfer)      | Strict (vs Best)  | 29            | 2    | 6.9%     | 3.6%      | 1.04x        | 1.07x       |
| Ours (no opt + xfer)      | Soft (vs Worst)   | 29            | 20   | 69.0%    | 66.2%     | 24.15x       | 383.52x     |
| Random baseline           | Strict (vs Best)  | 227           | 9    | 4.0%     | 30.2%     | 1.53x        | 2.37x       |
| Random baseline           | Soft (vs Worst)   | 227           | 112  | 49.3%    | 40.5%     | 3.55x        | 135.90x     |
| LLM baseline              | Strict (vs Best)  | 114           | 21   | 18.4%    | 36.1%     | 1.94x        | 6.94x       |
| LLM baseline              | Soft (vs Worst)   | 114           | 84   | 73.7%    | 48.3%     | 14.24x       | 772.06x     |

### Overall Partitioning Results on the TPC-H Dataset

| Experiment                 | Comparison        | Total Queries | Wins | Win Rate | Mean Gain | Mean Speedup | Max Speedup |
|---------------------------|-------------------|---------------|------|----------|-----------|--------------|-------------|
| Ours (opt + xfer)         | Strict (vs Best)  | 9             | 4    | 44.4%    | 28.2%     | 1.98x        | 4.47x       |
| Ours (opt + xfer)         | Soft (vs Worst)   | 9             | 8    | 88.9%    | 69.6%     | 29.33x       | 205.01x     |
| Ours (opt, no xfer)       | Strict (vs Best)  | 189           | 50   | 26.5%    | 42.2%     | 2.52x        | 7.26x       |
| Ours (opt, no xfer)       | Soft (vs Worst)   | 189           | 164  | 86.8%    | 77.5%     | 23.89x       | 528.34x     |
| Ours (no opt, no xfer)    | Strict (vs Best)  | 121           | 32   | 26.4%    | 38.8%     | 2.63x        | 6.78x       |
| Ours (no opt, no xfer)    | Soft (vs Worst)   | 121           | 103  | 85.1%    | 78.7%     | 21.41x       | 345.51x     |
| Ours (no opt + xfer)      | Strict (vs Best)  | 13            | 4    | 30.8%    | 40.9%     | 2.98x        | 5.97x       |
| Ours (no opt + xfer)      | Soft (vs Worst)   | 13            | 13   | 100.0%   | 74.2%     | 20.48x       | 160.43x     |
| Random baseline           | Strict (vs Best)  | 210           | 32   | 15.2%    | 47.5%     | 3.08x        | 17.69x      |
| Random baseline           | Soft (vs Worst)   | 210           | 135  | 64.3%    | 70.9%     | 15.82x       | 426.99x     |
| LLM baseline              | Strict (vs Best)  | 59            | 10   | 16.9%    | 24.5%     | 1.82x        | 6.39x       |
| LLM baseline              | Soft (vs Worst)   | 59            | 45   | 76.3%    | 60.4%     | 18.49x       | 316.30x     |

