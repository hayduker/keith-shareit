#!/bin/bash

cargo build

cp target/debug/d2j a/
cp target/debug/d2j b/

rm -rf b/payload

cd a/

./d2j send payload
