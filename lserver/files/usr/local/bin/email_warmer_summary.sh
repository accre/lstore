#!/usr/bin/env bash

WARMER_LOG="/lio/log/warmer_run.log"
WARMER_LAST_EMAIL="/lio/log/warmer_last_email.log"
WARMER_SUMMARY_EXEC="/usr/local/bin/warmer_summary.sh"
NAME=${HOSTNAME}
[ "${DOCKER_NAME}" != "" ] && NAME="${DOCKER_NAME} on ${HOSTNAME}"

# Check if it's a different day since the last email
if [ -e ${WARMER_LAST_EMAIL} ]; then
	LAST_EMAIL_EPOCH_DAYS=$(head -n 1 ${WARMER_LAST_EMAIL})
else
	LAST_EMAIL_EPOCH_DAYS="0"
fi

#GEt today's epoch day
TODAY_EPOCH_DAYS=$(expr $(date +%s) / 86400)

if [ "${TODAY_EPOCH_DAYS}" -le "${LAST_EMAIL_EPOCH_DAYS}" ]; then
	#Same day so nothing to do
	exit 0
fi

#If we made it here then send an email report

# First, let's generate the report
REPORT=$(mktemp)
${WARMER_SUMMARY_EXEC}  ${WARMER_LOG} | tee ${REPORT}
LINES=$(cat ${REPORT} | wc -l)
LINES=$((LINES-1))

FROM_ADDR="storage-nagios@accre.vanderbilt.edu"
TO_ADDR="reddnet-alerts@lists.accre.vanderbilt.edu"

SUBJECT="[${NAME}] $(cat ${REPORT} | head -n 1)"

BODY=$(mktemp)
cat ${REPORT} | tail -n ${LINES} > ${BODY}
TEXT_BODY=$(cat ${BODY})

# send email
# Create a temp file for the email
TMP=$(/bin/mktemp)

cat <<EOF >>${TMP}
Subject: ${SUBJECT}

${TEXT_BODY}

EOF

echo -e "To: ${TO_ADDR}" | cat ${TMP} | sendmail -f ${FROM_ADDR} -F "LIO Warmer Report" ${TO_ADDR}

rm -f ${REPORT} ${BODY} ${TMP}

#Update the timestamp of the email
echo ${TODAY_EPOCH_DAYS} > ${WARMER_LAST_EMAIL}

