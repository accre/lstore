#!/bin/bash

if [ "${1}" == "" ]; then
  echo "${0} warmer_log_file"
  exit 1
fi

WARMER=${1}
SUMMARY=/tmp/warmer.out
HRS_MAX="18"

#Check the timestamp on the warmer file
if [ -f ${WARMER} ]; then
  start=$(head -n 2 ${WARMER} | tail -n 1)
  warm_time=$(date --date="${start}" +%s)
else
  warm_time=0
fi
now=$(date +%s)
run_hrs=$(echo "scale=1; (${now} - ${warm_time}) / 3600" | bc)
time_text="Run Hrs: ${run_hrs}"
time_ok=$(echo "${run_hrs} < ${HRS_MAX}" | bc)

#Make the summary
[ -f ${SUMMARY} ] && /bin/rm ${SUMMARY}

awk '
BEGIN {
  tail = 0;
}

{
  if ((NR < 4) || (tail == 1)) {
     print $0;
  } else if (/------------/) {
     tail = 1;
  }
}
' ${WARMER} > ${SUMMARY}

#Now make sure it completed
summary_text=$(grep Submitted ${SUMMARY} | tr -s " ")

if [ "${summary_text}" == "" ]; then
   echo "STATUS: ERROR! Warmer failed to complete!"
   exit 1
fi

#Check for warming and write errors
value=$(echo ${summary_text} | tr -s " " | cut -f6 -d" ")
warm_ok=0;  [ "${value}" == "0" ] && warm_ok=1;
value=$(echo ${summary_text} | tr -s " " | cut -f9 -d" ")
write_ok=0;  [ "${value}" == "0" ] && write_ok=1;

#Get the RID summary
rid_sum=$(grep -F 'SUM (' /tmp/warmer.out | tr -s " " | cut -f2-5 -d " ")

#echo "time=${time_ok} warm=${warm_ok} write=${write_ok}"

#Print status and summary
if [ "${time_ok}" == "1" ] && [ "${warm_ok}" == "1" ] && [ "${write_ok}" == "1" ]; then
  echo "STATUS: Ok -- ${time_text} ${summary_text} ${rid_sum}"
else
  echo "STATUS: ERROR -- ${time_text} ${summary_text} ${rid_sum}"
fi

cat ${SUMMARY}
