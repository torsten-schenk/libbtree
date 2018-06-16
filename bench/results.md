## Comparison with QMap and std::map
file: `vs_qmap.cpp`

Elements: 1,000,000

btree order: 15

Runs: 5

Notable flags: -O3 -fPIC

Result times are given in msec

Note, that there is a huge difference in table "random insert elements", row "btree" when compiling the benchmark for qt4 vs qt5.
The btree seems to benefit from memory previously allocated by qt5 qmap.

### random access
| item | min | max | avg | median |
|----------|----:|----:|----:|----:|
| **qmap 4.8** | 921 | 923 | 922 | 922 |
| **qmap 5.0** | 695 | 697 | 696 | 696 |
| **std::map** | 680 | 681 | 680 | 681 |
| **btree** | 504 | 506 | 505 | 505 |

### first access
| item | min | max | avg | median |
|----------|----:|----:|----:|----:|
| **qmap 4.8** | 15 | 16 | 15 | 15 |
| **qmap 5.0** | 31 | 31 | 31 | 31 |
| **std::map** | 30 | 30 | 30 | 30 |
| **btree** | 109 | 110 | 109 | 109 |

### last access
| item | min | max | avg | median |
|----------|----:|----:|----:|----:|
| **qmap 4.8** | 80 | 82 | 81 | 81 |
| **qmap 5.0** | 65 | 65 | 65 | 65 |
| **std::map** | 62 | 63 | 62 | 62 |
| **btree** | 123 | 125 | 124 | 124 |

### middle access
| item | min | max | avg | median |
|----------|----:|----:|----:|----:|
| **qmap 4.8** | 80 | 99 | 86 | 84 |
| **qmap 5.0** | 35 | 36 | 35 | 35 |
| **std::map** | 34 | 34 | 34 | 34 |
| **btree** | 123 | 125 | 123 | 123 |

### random remove elements
| item | min | max | avg | median |
|----------|----:|----:|----:|----:|
| **qmap 4.8** | 1216 | 1531 | 1451 | 1508 |
| **qmap 5.0** | 896 | 1109 | 1065 | 1107 |
| **std::map** | 840 | 1040 | 998 | 1037 |
| **btree** | 681 | 718 | 705 | 707 |

### remove first element
| item | min | max | avg | median |
|----------|----:|----:|----:|----:|
| **qmap 4.8** | 132 | 142 | 137 | 140 |
| **qmap 5.0** | 204 | 207 | 205 | 205 |
| **std::map** | 151 | 153 | 152 | 152 |
| **btree** | 227 | 229 | 228 | 228 |

### remove last element
| item | min | max | avg | median |
|----------|----:|----:|----:|----:|
| **qmap 4.8** | 282 | 328 | 305 | 308 |
| **qmap 5.0** | 243 | 245 | 244 | 244 |
| **std::map** | 149 | 152 | 150 | 150 |
| **btree** | 200 | 203 | 201 | 201 |

### append elements
| item | min | max | avg | median |
|----------|----:|----:|----:|----:|
| **qmap 4.8** | 100 | 130 | 112 | 106 |
| **qmap 5.0** | 268 | 270 | 268 | 269 |
| **std::map** | 274 | 277 | 275 | 275 |
| **btree** | 212 | 219 | 213 | 213 |

### prepend elements
| item | min | max | avg | median |
|----------|----:|----:|----:|----:|
| **qmap 4.8** | 110 | 137 | 121 | 119 |
| **qmap 5.0** | 274 | 276 | 275 | 275 |
| **std::map** | 249 | 254 | 251 | 251 |
| **btree** | 218 | 222 | 219 | 219 |

### random insert elements
| item | min | max | avg | median |
|----------|----:|----:|----:|----:|
| **qmap 4.8** | 1355 | 1391 | 1367 | 1366 |
| **qmap 5.0** | 845 | 847 | 845 | 845 |
| **std::map** | 593 | 595 | 593 | 594 |
| **btree** | 709 | 1271 | 822 | 710 |

