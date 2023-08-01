#!/bin/bash

for i in {1..1000}
do
    echo "Test $i"
    VERBOSE=0 go test
done
```
