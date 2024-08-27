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
python_script="${PROJECT_DIR}/create_scripts/create_sql.py"
shell_script_dir="${PROJECT_DIR}/scripts"



while true; do
    echo "Select one of the following options:"
    echo -e "${Purple}1) Generate Truncate and Sync files${NC}"
    echo -e "${Green}2) Generate only Sync files${NC}"
    echo -e "${Yellow}3) Move Insert CSV to mysql-files directory${NC}"
    echo -e "${Yellow}4) Move Special insert CSV to  mysql-files directory${NC}"
    echo -e "${Black}5) Move Special insert CSV to the tests directory${NC}"
    echo -e "${Red}6) Clean mysql-files directory${NC}"
    echo -e "${Black}7) Clean tests directory${NC}"
    echo -e "${Red}8) Run Insert Script${NC}"
    echo -e "${Red}9) Run Special Insert Script${NC}"
    read -p "Enter your choice (or 'exit' to quit): " choice
    
    if [[ $choice == "exit" ]]; then
        echo "Exiting..."
        break
    fi

    # Process the user's choice
    case $choice in
        1)
            read -p "Are you sure you want to sync database with truncate option? (y/n): " truncate_choice

            if [[ $truncate_choice == "y" ]]; then
                echo "Syncing database with truncate option..."
                export SCHEMA_FILE="${PROJECT_DIR}/schema/smartmate-truncate-sync.json"
                $PYTHONPATH $python_script
            fi

            ;;
        2)
            read -p "Are you sure you only want to sync database? (y/n): " truncate_choice
            if [[ $truncate_choice == "y" ]]; then
                echo "Syncing database..."
                export SCHEMA_FILE="${PROJECT_DIR}/schema/smartmate-sync.json"
                $PYTHONPATH $python_script
            fi
            ;;
        3)
            echo "Moving insert CSV to mysql-files directory..."
            $shell_script_dir/move_insert_csv_2_mysql-files.sh
            ;;
        4) 
            echo "Moving special insert CSV to mysql-files directory..."
            $shell_script_dir/move_special_insert_csv_2_mysql-files.sh
            ;;
        5)
            echo "Moving special insert CSV to tests directory..."
            $shell_script_dir/move_special_insert_csv_2_tests.sh
            ;;
        6)  
            echo "Cleaning mysql-files directory..."
            $shell_script_dir/cleanup.sh
            ;;
        7) 
            echo "Cleaning tests directory..."
            $shell_script_dir/tests-cleanup.sh
            ;;
        8)
            read -p "Are you sure you want to start insert process? (y/n): " u_sure_bro
            if [[ $u_sure_bro == "y" ]]; then
                echo "Running Insert Script..."
                $shell_script_dir/insert_sql.sh
            fi
            ;;
        9)
            read -p "Are you sure you want to start special insert process? (y/n): " u_sure_bro
            if [[ $u_sure_bro == "y" ]]; then
                echo "Running Special Insert Script..."
                $shell_script_dir/special_insert_sql.sh
            fi
            ;;
        *)
            echo "Invalid choice"
            ;;
    esac
done