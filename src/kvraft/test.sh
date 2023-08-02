#!/bin/bash

for i in {1..100}
do
    echo "Test $i"
    VERBOSE=0 go test -run 3B
done
```
