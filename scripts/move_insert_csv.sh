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

SECONDS=0

set -a ; source ../.env ; set +a

INSERT_DIR="$SOURCE_DIR/insert"

start() {
    set -e 

    cd "$INSERT_DIR" || { echo -e "${Red}Error: Source directory not found.${NC}"; exit 1; }

    echo -e "${Black}---------------------------------------------${NC}"
    echo -e "${Green}Moving insert csv data started at $(date)${NC}"
    echo -e "${Black}---------------------------------------------${NC}\n"

    for dir in */; do
        table_name=$(basename "$dir")
        echo -e "${Yellow}Moving table csv: '${Purple}$table_name${Yellow}'${NC}"

        csv_file="${dir}insert-${table_name#* -}.csv"
        sql_file="${dir}${table_name}_load_csv.sql"
        
        echo -e " - ${Cyan}Looking for CSV file: ${White}$csv_file${NC}"
        if [ ! -f "$csv_file" ]; then
            echo -e "${Red}Error: CSV file $csv_file not found. Exiting...${NC}\n"
            exit 1
        fi
        
        cp "$csv_file" "$DEST_DIR/"
        csv_file_name=$(basename "$csv_file")
        chmod "$PERMISSIONS" "$DEST_DIR/$csv_file_name"
        chgrp "$GROUP" "$DEST_DIR/$csv_file_name"
                
        echo -e "${Green}Finished moving table csv '${Purple}$table_name${Green}'${NC}\n"
    done
}

finish() {
    minutes=$((SECONDS / 60))
    seconds=$((SECONDS % 60))
    backup_duration=$(printf "%d minute(s) and %d second(s)" $minutes $seconds)
    echo -e "${Black}---------------------------------------------${NC}"
    echo -e "${Green}Moving data finished at $(date)${NC}"
    echo -e "${Black}---------------------------------------------${NC}"
    echo -e "${Cyan}Total duration: ${White}${backup_duration}${NC}"
    echo -e "${Black}---------------------------------------------${NC}\n"
    exit 0
}

trap finish EXIT

start
