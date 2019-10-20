#!/bin/bash

./bin/cyclosa --removetempfiles=true --tempfilesquantity 10 --keylimit 5 integrationtest/testdata.in /tmp/out

sort integrationtest/testdata.out-golden > /tmp/golden-sorted-out
sort /tmp/out > /tmp/sorted-out

diff /tmp/golden-sorted-out /tmp/sorted-out

exit $?
