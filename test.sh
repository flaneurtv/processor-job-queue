#/!bin/ash
FILENAME=$1

while read line
do
    sleep 0.2
    echo $line | NOW=`date -u +"%FT%T.000Z"` envsubst
done <$FILENAME | tee /dev/stderr | SERVICE_UUID=SSSSSSSS-1285-4E4C-A44E-AAAABBBB0000 SERVICE_NAME=job_queue SERVICE_HOST=machine NAMESPACE_LISTENER=flaneur NAMESPACE_PUBLISHER=flaneur node processor.js
