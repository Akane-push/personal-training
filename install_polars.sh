#!/bin/sh

if grep -q "avx2" /proc/cpuinfo; then
    echo "AVX2 detected. Installing standard polars..."
    pip install polars
else
    echo "AVX2 not detected. Installing polars[rtcompat]..."
    pip install polars[rtcompat]
fi