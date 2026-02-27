## Appendix A3 – Transfer cost model coefficients

Below is the table of the learned transfer cost coefficients used in the paper (all units in ms).

| Parameter              | DuckDB → DataFusion | DataFusion → DuckDB |
|------------------------|---------------------|---------------------|
| Intercept (a)         | 72.93               | 62.30               |
| Per Row (b)           | 8.30 × 10^-5        | 1.99 × 10^-4        |
| Per Byte Width (c)    | 1.10 × 10^-3        | 0.00                |
| Per Data Byte (d)     | 3.80 × 10^-7        | 9.38 × 10^-7        |
| RMSE (ms)             | 22.38               | 24.58               |
| R^2                   | 0.88                | 0.97                |

These coefficients are used by the linear transfer model:

`T(u, e_src, e_dst) = a + b * R_u + c * W_u + d * R_u * W_u`.

