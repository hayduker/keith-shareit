#!/bin/bash

cargo build

cp target/debug/keith-shareit a/
cp target/debug/keith-shareit b/

# rm -rf b/payload

cd a/

# ./keith-shareit send payload
./keith-shareit send