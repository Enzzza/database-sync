#!/bin/bash

# Regular Colors
Black='\033[0;30m'
Red='\033[0;31m'
Green='\033[0;32m'
Yellow='\033[0;33m'
Purple='\033[0;35m'
Cyan='\033[0;36m'
White='\033[0;37m'
NC='\033[0m'  # No Color

set -a ; source ../.env ; set +a


echo -e "${Black}---------------------------------------------${NC}"
echo -e "${Green}Cleanup started at $(date)${NC}"
echo -e "${Purple}Cleaning destination directory: ${White}$TEST_DEST_DIR/special_insert${NC}"
if [ -z "$(ls -A $TEST_DEST_DIR/special_insert)" ]; then
    echo -e "${Yellow}Destination directory is empty.${NC}"
else
    sudo find $TEST_DEST_DIR/special_insert -type f -print0 | sudo xargs -0 rm
    echo -e "${Green}Destination directory cleaned.${NC}"
fi
echo -e "${Green}Cleanup finished at $(date)${NC}"
echo -e "${Black}---------------------------------------------${NC}"
