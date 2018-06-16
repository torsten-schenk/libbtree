## Comparison with QMap
file: `vs_qmap.cpp`

Elements: 1,000,000

btree order: 15

Runs: 5

Optimization: -O3

Results:

### random access
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 795 | 796 | 795 |
| **btree** | 485 | 487 | 485 |

### first access
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 15 | 15 | 15 |
| **btree** | 108 | 108 | 108 |

### last access
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 90 | 92 | 90 |
| **btree** | 122 | 123 | 122 |

### middle access
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 65 | 76 | 68 |
| **btree** | 122 | 122 | 122 |

### random remove elements
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 1034 | 1371 | 1291 |
| **btree** | 649 | 680 | 673 |

### remove first element
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 123 | 132 | 128 |
| **btree** | 228 | 230 | 229 |

### remove last element
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 276 | 319 | 297 |
| **btree** | 186 | 195 | 188 |

### append elements
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 99 | 128 | 109 |
| **btree** | 208 | 222 | 211 |

### prepend elements
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 120 | 145 | 128 |
| **btree** | 208 | 227 | 214 |

### random insert elements
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 1149 | 1183 | 1159 |
| **btree** | 815 | 875 | 862 |


