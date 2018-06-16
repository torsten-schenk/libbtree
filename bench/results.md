## Comparison with QMap
file: `vs_qmap.cpp`

Elements: 1,000,000

btree order: 15

Runs: 5

Results:

### random access
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 1085 | 1086 | 1085 |
| **btree** | 658 | 660 | 659 |

### first access
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 142 | 144 | 142 |
| **btree** | 270 | 272 | 271 |

### last access
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 421 | 422 | 421 |
| **btree** | 307 | 308 | 307 |

### middle access
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 644 | 645 | 644 |
| **btree** | 372 | 377 | 374 |

### append elements
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 540 | 569 | 553 |
| **btree** | 484 | 497 | 488 |

### prepend elements
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 191 | 213 | 202 |
| **btree** | 713 | 720 | 717 |

### random insert elements
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 1449 | 1520 | 1482 |
| **btree** | 1123 | 1267 | 1237 |


