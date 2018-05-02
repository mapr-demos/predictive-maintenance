#!/usr/bin/env bash
# Step 3 - Save IoT data stream to OpenTSDB:
/opt/mapr/kafka/kafka-*/bin/kafka-console-consumer.sh --new-consumer --topic /apps/factory:mqtt --bootstrap-server not.applicable:0000 | while read line; do echo $line | jq -r "to_entries | map(\"\(.key) \(.value | tostring)\") | {t: .[0], x: .[]} | .[]" | paste -d ' ' - - | awk '{system("curl -X POST --data \x27{\"metric\": \""$3"\", \"timestamp\": "$2", \"value\": "$4", \"tags\": {\"host\": \"localhost\"}}\x27 http://localhost:4242/api/put")}'; echo -n "."; done
