# /
## build
```
make build # and checkout the ./bin directory
```

## run a simple (barely smoke) integration test
```
make integrationtest
```

## usage
```
# example
./bin/cyclosa --removetempfiles=false --tempfilesquantity 10 --keylimit 1000 /tmp/test-input.log /tmp/stat-out
# removetempfiles=false | true; (default true)
# tempfilesquantity specifies how many temp files are used; (default 10)
# keylimit specifies the quantity in-memory key-value records; (default 100)
# /tmp/test-input.log - input file
# /tmp/stat-out - output file
```
