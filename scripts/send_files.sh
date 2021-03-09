#!/bin/bash

if [[ -z "${VMNAME}" ]]
then
    echo "Please provide a vmname"
    echo "Usage: export VMNAME=<vmname> && ./send_files.sh"
    exit  
fi

# Zip source code
tar -czvf ../src.tar.gz ../src

# Send source code to master
scp ../src.tar.gz user@$VMNAME:.
# Uncompressed source folder
ssh user@$VMNAME 'tar -xzf ./src.tar.gz && rm ./src.tar.gz'
