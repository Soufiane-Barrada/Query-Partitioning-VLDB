## Appendix A7 – Query runtime percentile buckets

Below are the tables of the runtime percentile buckets.

### All datasets — runtime percentiles

| Runtime Bucket | Min Time (s) | Max Time (s) | Queries |
|---------------|--------------|--------------|---------|
| p0–p25        | 0.005166     | 0.447272     | 187     |
| p25–p50       | 0.448991     | 0.944290     | 187     |
| p50–p75       | 0.950450     | 2.781910     | 187     |
| p75–p100      | 2.830881     | 346.950753   | 187     |

### Query categorization by best single-engine runtime per dataset

| Dataset      | Runtime Bucket | Min Time (s) | Max Time (s) | Queries per Bucket |
|-------------|----------------|--------------|--------------|--------------------|
| JOB (IMDb)  | p0–p25         | 0.005166     | 0.303222     | 57                 |
| JOB (IMDb)  | p25–p50        | 0.310449     | 0.648822     | 57                 |
| JOB (IMDb)  | p50–p75        | 0.652335     | 1.562250     | 57                 |
| JOB (IMDb)  | p75–p100       | 1.641054     | 65.895833    | 57                 |
| STACK (SO)  | p0–p25         | 0.089395     | 0.560072     | 69                 |
| STACK (SO)  | p25–p50        | 0.562674     | 0.954993     | 69                 |
| STACK (SO)  | p50–p75        | 0.960360     | 3.234004     | 68                 |
| STACK (SO)  | p75–p100       | 3.253119     | 346.950753   | 69                 |
| TPC-H       | p0–p25         | 0.050763     | 0.527936     | 62                 |
| TPC-H       | p25–p50        | 0.531176     | 1.152726     | 61                 |
| TPC-H       | p50–p75        | 1.157020     | 3.460905     | 61                 |
| TPC-H       | p75–p100       | 3.561773     | 159.777845   | 61                 |

