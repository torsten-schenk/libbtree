## Comparison with QMap
file: `vs_qmap.cpp`

Elements: 1,000,000

btree order: 15

Runs: 5

Results:

### append elements
| **item** | min | max | avg |
|----------|----:|----:|----:|
| *qmap* | 544 | 588 | 565 |
| *btree* | 494 | 513 | 507 |

### prepend elements
| **item** | min | max | avg |
|----------|----:|----:|----:|
| *qmap* | 195 | 223 | 212 |
| *btree* | 716 | 733 | 728 |

### random insert elements
| **item** | min | max | avg |
|----------|----:|----:|----:|
| *qmap* | 1471 | 1538 | 1503 |
| *btree* | 1120 | 1263 | 1233 |

### random access
| **item** | min | max | avg |
|----------|----:|----:|----:|
| *qmap* | 1718 | 1769 | 1742 |
| *btree* | 18 | 18 | 18 |

