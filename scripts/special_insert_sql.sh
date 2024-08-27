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


SPECIAL_UPDATE_DIR="$SOURCE_DIR/special_insert"

start() {
    set -e
    cd "$SPECIAL_UPDATE_DIR" || { echo -e "${Red}Error: Source directory not found.${NC}"; exit 1; }

    echo -e "${Black}---------------------------------------------${NC}"
    echo -e "${Green}Inserting data started at $(date)${NC}"
    echo -e "${Black}---------------------------------------------${NC}\n"

    for dir in */; do
        table_name=$(basename "$dir")
        echo -e "${Yellow}Processing table: '${Purple}$table_name${Yellow}'${NC}"

        sql_file="${dir}${table_name}_load_csv.sql"
        
        echo -e " - ${Cyan}Looking for SQL file: ${White}$sql_file${NC}"
        if [ ! -f "$sql_file" ]; then
            echo -e "${Red}Error: SQL file $sql_file not found. Skipping...${NC}\n"
            exit 1
        fi
        
        mysql -u$GALERA_USER -p$GALERA_PASSWORD -D $GALERA_DATABASE < "$sql_file"
        
        echo -e "${Green}Finished inserting table '${Purple}$table_name${Green}'${NC}\n"
    done
}

finish() {
    minutes=$((SECONDS / 60))
    seconds=$((SECONDS % 60))
    backup_duration=$(printf "%d minute(s) and %d second(s)" $minutes $seconds)
    echo -e "${Black}---------------------------------------------${NC}"
    echo -e "${Green}Inserting data finished at $(date)${NC}"
    echo -e "${Black}---------------------------------------------${NC}"
    echo -e "${Cyan}Total duration: ${White}${backup_duration}${NC}"
    echo -e "${Black}---------------------------------------------${NC}\n"
    exit 0
}

trap finish EXIT

start
