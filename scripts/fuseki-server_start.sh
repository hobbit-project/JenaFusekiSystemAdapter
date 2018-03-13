#!/bin/sh

rm /jenafuseki/fuseki.log 2> /dev/null

echo "Starting Jena Fuseki Server (--loc --localhost --update /ds)..."
/jena-fuseki/fuseki-server --loc=/jenafuseki/data --localhost --update /ds > /jenafuseki/fuseki.log 2>&1 &
seconds_passed=0

# wait until jenafuseki is ready
until grep -m 1 "on port 3030" /jenafuseki/fuseki.log
do
   sleep 1
   seconds_passed=$((seconds_passed+1))
   echo $seconds_passed >> out.txt
   if [ $seconds_passed -gt 120 ]; then
      echo "Could not start Jena Fuseki Server. Timeout: [2 min]"
      echo "Exiting..."
      exit
   fi
done
