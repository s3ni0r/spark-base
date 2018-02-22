#!/bin/bash
# debug
#set -x
# abort if any error occured
set -e
# raise an error when an undefined variable has been used
set -u
# abort when a pipe failed = bash only: doesn't work in POSIX sh
if [ "${BASH:-}" = '/bin/bash' ]; then set -o pipefail; fi

#############################
#       ENV VARIABLES       #
#############################
WEBHDFS_URL=${WEBHDFS_URL}
WEBHDFS_USER=${TECHNICAL_USER}
WEBHDFS_PASSWORD=${TECHNICAL_PASSWORD}
DEFAULT_FOLDER_PERMISSION=750
BASE_HDFS_DIRECTORY=${BASE_HDFS_DIRECTORY}
LOCAL_JOB_FOLDER=$(PWD)/$1
REMOTE_JOB_FOLDER=$BASE_HDFS_DIRECTORY/$2


create_hdfs_folder(){
    FOLDER_PATH=$1
#    echo "$WEBHDFS_URL/$FOLDER_PATH?op=MKDIRS&permission=$DEFAULT_FOLDER_PERMISSION"
    curl -i -k -u $WEBHDFS_USER:$WEBHDFS_PASSWORD -X PUT "$WEBHDFS_URL/$FOLDER_PATH?op=MKDIRS&permission=$DEFAULT_FOLDER_PERMISSION"
}

upload_file()
{
    HDFS_FILE_PATH=$1
    LOCAL_FILE_PATH=$2
#    echo "$WEBHDFS_URL/$HDFS_FILE_PATH?op=CREATE&overwrite=true"
#    echo "$LOCAL_JOB_FOLDER/$LOCAL_FILE_PATH"
    query=`curl -i -k -u $WEBHDFS_USER:$WEBHDFS_PASSWORD -X PUT "$WEBHDFS_URL/$HDFS_FILE_PATH?op=CREATE&overwrite=true" | grep Location | awk -F ' ' '{print $2}' | sed 's/[\r\n]//g'`
    curl -i -k -u $WEBHDFS_USER:$WEBHDFS_PASSWORD -X PUT -T "$LOCAL_JOB_FOLDER/$LOCAL_FILE_PATH" "$query"
}

sync_job_dir(){
    DIRECTORY=$1
    for entry in $( find ${DIRECTORY});
    do
      if [ -f "$entry" ];then
#        echo "this is a file ${entry#"$LOCAL_JOB_FOLDER/"}"
        upload_file $REMOTE_JOB_FOLDER/${entry#"$LOCAL_JOB_FOLDER/"} ${entry#"$LOCAL_JOB_FOLDER/"}
      fi
      if [ -d "$entry" ];then
#        echo "this is a folder ${entry#"$LOCAL_JOB_FOLDER/"}"
        create_hdfs_folder $REMOTE_JOB_FOLDER/${entry#"$LOCAL_JOB_FOLDER/"}
      fi
    done
}


#####################
#       MAIN        #
#####################
# Create remote folder baser on script parameter (2nd)
create_hdfs_folder $REMOTE_JOB_FOLDER
# Sync all local files to remote hdfs
sync_job_dir "$LOCAL_JOB_FOLDER/*"