#!/bin/sh

mkdir -p ./sample.d/

printf 'wrld' > ./sample.d/helo.txt

export PGHOST=127.0.0.1
export PGUSER=postgres
export PGPORT=5433
export PGDATABASE=go_fs2pg2lo

echo writing small content...
time ./file2largeobj -file ./sample.d/helo.txt

echo
echo creating bigger file...
dd \
	if=/dev/urandom \
	of=./sample.d/heavy.dat \
	bs=16777216 \
	count=1

echo
echo writing large content...
time ./file2largeobj -file ./sample.d/heavy.dat
