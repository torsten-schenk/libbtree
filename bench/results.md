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
| **qmap** | 720 | 722 | 720 |
| **btree** | 452 | 453 | 452 |

### first access
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 13 | 13 | 13 |
| **btree** | 108 | 108 | 108 |

### last access
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 89 | 89 | 89 |
| **btree** | 121 | 122 | 121 |

### middle access
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 66 | 73 | 68 |
| **btree** | 121 | 122 | 121 |

### random remove elements
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 940 | 1255 | 1186 |
| **btree** | 601 | 631 | 624 |

### remove first element
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 123 | 133 | 128 |
| **btree** | 224 | 225 | 224 |

### remove last element
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 266 | 319 | 290 |
| **btree** | 184 | 194 | 189 |

### append elements
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 97 | 126 | 107 |
| **btree** | 203 | 217 | 206 |

### prepend elements
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 49 | 55 | 51 |
| **btree** | 224 | 230 | 227 |

### random insert elements
| item | min | max | avg |
|----------|----:|----:|----:|
| **qmap** | 1041 | 1073 | 1052 |
| **btree** | 678 | 822 | 792 |

