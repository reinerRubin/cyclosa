#!/bin/bash

echo "hi"

./bin/cyclosa --removetempfiles=false --tempfilesquantity 10 --keylimit 1000 integrationtest/testdata.in /tmp/out

sort integrationtest/testdata.out-golden > /tmp/golden-sorted-out
sort /tmp/out > /tmp/sorted-out

diff /tmp/golden-sorted-out /tmp/sorted-out

exit $?
