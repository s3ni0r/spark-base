#!/bin/bash
# debug
#set -x
# abort if any error occured
set -e
# disable globbing
#set -f
# disable splitting
#IFS=''
# raise an error when an undefined variable has been used
set -u
# abort when a pipe failed = bash only: doesn't work in POSIX sh
if [ "${BASH:-}" = '/bin/bash' ]; then set -o pipefail; fi

#############################
#       ENV VARIABLES       #
#############################
WEBHDFS_URL=${WEBHDFS_URL:?"You need to set up WEBHDFS_URL env variable (export WEBHDFS_URL=...)"}
WEBHDFS_USER=${TECHNICAL_USER:?"You need to set up TECHNICAL_USER env variable (export TECHNICAL_USER=...)"}
WEBHDFS_PASSWORD=${TECHNICAL_PASSWORD:?"You need to set up TECHNICAL_PASSWORD env variable (export TECHNICAL_PASSWORD=...)"}
DEFAULT_FOLDER_PERMISSION=750
BASE_HDFS_DIRECTORY=${BASE_HDFS_DIRECTORY:?"You need to set up BASE_HDFS_DIRECTORY env variable (export BASE_HDFS_DIRECTORY=...)"}
LOCAL_JOB_FOLDER=`realpath ${1:?"You need to specify local directory to sync into hdfs"}`
REMOTE_JOB_FOLDER=${BASE_HDFS_DIRECTORY}/${2:?"You need to specify a destination hdfs folder name"}

#############################
#       TEMP VARS           #
#############################
JOB_ID=""

#########################################
#   create_hdfs_folder myJobFolder      #
#########################################
create_folder(){
    FOLDER_PATH=$1
#    echo "$WEBHDFS_URL/$FOLDER_PATH?op=MKDIRS&permission=$DEFAULT_FOLDER_PERMISSION"
    curl -i -k -u ${WEBHDFS_USER}:${WEBHDFS_PASSWORD} -X PUT "$WEBHDFS_URL/$FOLDER_PATH?op=MKDIRS&permission=$DEFAULT_FOLDER_PERMISSION"
}

upload_file()
{
    HDFS_FILE_PATH=$1
    LOCAL_FILE_PATH=$2
#    echo "$WEBHDFS_URL/$HDFS_FILE_PATH?op=CREATE&overwrite=true"
#    echo "$LOCAL_JOB_FOLDER/$LOCAL_FILE_PATH"
    query=`curl -i -k -u ${WEBHDFS_USER}:${WEBHDFS_PASSWORD} -X PUT "$WEBHDFS_URL/$HDFS_FILE_PATH?op=CREATE&overwrite=true" | grep Location | awk -F ' ' '{print $2}' | sed 's/[\r\n]//g'`
    curl -i -k -u ${WEBHDFS_USER}:${WEBHDFS_PASSWORD} -X PUT -T "$LOCAL_JOB_FOLDER/$LOCAL_FILE_PATH" "$query"
}

sync_job_dir(){
    DIRECTORY=$1
    for entry in $( find ${DIRECTORY});
    do
      if [ -f "$entry" ];then
#        echo "this is a file ${entry#"$LOCAL_JOB_FOLDER/"}"
        upload_file ${REMOTE_JOB_FOLDER}/${entry#"$LOCAL_JOB_FOLDER/"} ${entry#"$LOCAL_JOB_FOLDER/"}
      fi
      if [ -d "$entry" ];then
#        echo "this is a folder ${entry#"$LOCAL_JOB_FOLDER/"}"
        create_folder ${REMOTE_JOB_FOLDER}/${entry#"$LOCAL_JOB_FOLDER/"}
      fi
    done
}


launch_job(){
#    echo "launch_job"
#    echo ${LOCAL_JOB_FOLDER}/job.properties.xml
    JOB_ID=`curl -k -u ${OOZIE_USER}:${OOZIE_PASSWORD} -H Content-Type:application/xml -T "${LOCAL_JOB_FOLDER}/job.properties.xml" -X POST "$OOZIE_URL/jobs?action=start" | jq ".id"`
    sleep 5
}

display_logs(){
#    echo "display_logs"
    temp_job_id=${JOB_ID%\"}
    proper_job_id=${temp_job_id#\"}
    urls=`curl -k -u ${OOZIE_USER}:${OOZIE_PASSWORD} -X GET "$OOZIE_URL/job/$proper_job_id?show=info" | jq '.actions[].consoleUrl'`
    for url in ${urls};
    do
      temp_url=${url%\"}
      proper_url=${temp_url#\"}
      if [[ ${proper_url} == http* ]];then
         echo ${proper_url}
        /usr/bin/open -a /Applications/Google\ Chrome.app $proper_url
      fi
    done
}

#####################
#       MAIN        #
#####################

create_folder $REMOTE_JOB_FOLDER
sync_job_dir "$LOCAL_JOB_FOLDER/*"
launch_job
display_logs