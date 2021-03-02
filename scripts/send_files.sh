#!/bin/bash

if [[ -z "${HOSTNAME}" ]]
then
    echo "Please provide a hostname"
    echo "Usage: export HOSTNAME=<hostname> && ./send_module.sh"
    exit  
fi

# Zip source code
tar -czvf ../src.tar.gz ../src

# Send source code to master
scp ../src.tar.gz user@$HOSTNAME:.
# Uncompressed source folder
ssh user@$HOSTNAME 'tar -xzf ./src.tar.gz && rm ./src.tar.gz'
