#!/bin/bash
for i in {1..2000}
   do
      key=`date +%s | shasum | base64 | head -c 32 ; echo`
      value=`date +%s | shasum | base64 | head -c 32 ; echo`
      echo "set $key $value" | redis-cli -p 22122 &
      # echo "get $key" | redis-cli -p 22122 > /dev/null  &
   done

