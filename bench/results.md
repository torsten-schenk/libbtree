## Comparison with QMap and std::map
file: `vs_qmap.cpp`

Elements: 1,000,000

btree order: 15

Runs: 5

Optimization: -O3

Results:

### random access
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap 4.8** | 922 | 924 | 923 |
| **std::map** | 656 | 658 | 656 |
| **btree** | 501 | 503 | 501 |

### first access
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap 4.8** | 12 | 12 | 12 |
| **std::map** | 29 | 29 | 29 |
| **btree** | 109 | 110 | 109 |

### last access
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap 4.8** | 66 | 67 | 66 |
| **std::map** | 62 | 63 | 62 |
| **btree** | 124 | 126 | 124 |

### middle access
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap 4.8** | 102 | 110 | 106 |
| **std::map** | 34 | 36 | 34 |
| **btree** | 123 | 125 | 123 |

### random remove elements
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap 4.8** | 1219 | 1532 | 1451 |
| **std::map** | 798 | 1016 | 972 |
| **btree** | 668 | 700 | 693 |

### remove first element
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap 4.8** | 132 | 141 | 137 |
| **std::map** | 151 | 154 | 153 |
| **btree** | 227 | 230 | 228 |

### remove last element
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap 4.8** | 280 | 322 | 302 |
| **std::map** | 148 | 152 | 149 |
| **btree** | 197 | 201 | 198 |

### append elements
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap 4.8** | 99 | 133 | 111 |
| **std::map** | 220 | 221 | 220 |
| **btree** | 213 | 223 | 215 |

### prepend elements
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap 4.8** | 111 | 140 | 122 |
| **std::map** | 233 | 234 | 233 |
| **btree** | 214 | 216 | 214 |

### random insert elements
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap 4.8** | 1357 | 1394 | 1368 |
| **std::map** | 608 | 610 | 608 |
| **btree** | 738 | 1036 | 976 |

