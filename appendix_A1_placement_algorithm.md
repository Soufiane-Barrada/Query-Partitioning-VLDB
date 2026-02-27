## Appendix A1 – Placement Algorithm (Critical-Path DP)

This file gives the complete pseudocode for the **Placement Algorithm under a cut budget K**.

---

### Algorithm: Placement Algorithm under a cut budget K

We write `ch(v)` for the children of node `v`, and `E` for the set of engines.

```text
Input:
  - Operator tree G = (V, E) rooted at r
  - Engines 𝓔
  - Cut budget K
  - Predicted self-times t(v, e) for every node v ∈ V and engine e ∈ 𝓔
  - Transfer model 𝒯(u, e_src, e_dst) for every edge (v, u) and engine pair

Output:
  - Minimal estimated latency min_{e ∈ 𝓔} dp[r][e][K]

1:  Postorder traverse all nodes v ∈ V
2:  for each v ∈ V in postorder do
3:    for each e ∈ 𝓔 do
4:      if ch(v) is empty then            ▷ Leaf case
5:        for k = 0 to K do
6:          dp[v][e][k] ← t(v, e)
7:        end for
8:      else
9:        ▷ Step 1: Precompute B_u[k] for each child u
10:       for each u ∈ ch(v) do
11:         for k = 0 to K do
12:           best ← dp[u][e][k]          ▷ No cut on edge (v, u)
13:           if k ≥ 1 then
14:             alt ←  min_{e' ∈ 𝓔 \ {e}} ( dp[u][e'][k-1] + 𝒯(u, e', e) )
15:             best ← min(best, alt)
16:           end if
17:           B_u[k] ← best
18:         end for
19:       end for

20:       ▷ Step 2: Merge children under MAX semantics
21:       for k = 0 to K do
22:         A[k] ← 0
23:       end for
24:       for each u ∈ ch(v) do           ▷ Incremental fold over children
25:         for k = 0 to K do
26:           A_new[k] ← +∞
27:         end for
28:         for k = 0 to K do
29:           for i = 0 to k do
30:             A_new[k] ← min( A_new[k], max( A[i], B_u[k-i] ) )
31:           end for
32:         end for
33:         A ← A_new
34:       end for

35:       ▷ Step 3: Add self-time of v on engine e
36:       for k = 0 to K do
37:         dp[v][e][k] ← t(v, e) + A[k]
38:       end for
39:     end if
40:   end for
41: end for

42: return  min_{e ∈ 𝓔} dp[r][e][K]
```


