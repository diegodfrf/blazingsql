#!/bin/bash
# Usage: ./snowflake_odbc_setup.sh

export GREEN='\033[0;32m'
export RED='\033[0;31m'
BOLDGREEN="\e[1;${GREEN}"
ITALICRED="\e[3;${RED}"
ENDCOLOR="\e[0m"

if [ ! -z $CONDA_PREFIX ]; then
    echo -e "${GREEN}Installing snowflake odbc driver${ENDCOLOR}"
    wget https://sfc-repo.snowflakecomputing.com/odbc/linux/2.23.2/snowflake_linux_x8664_odbc-2.23.2.tgz
    tar xf snowflake_linux_x8664_odbc-2.23.2.tgz -C $CONDA_PREFIX/opt
    echo -e "${GREEN}snowflake odbc driver installed in $CONDA_PREFIX/opt${ENDCOLOR}"
    echo -e "${GREEN}creating odbcinst in directory $CONDA_PREFIX/etc/odbcinst.ini${ENDCOLOR}"
    cat >> $CONDA_PREFIX/etc/odbcinst.ini << ODBCINST_INI
[SnowflakeDSIIDriver]
APILevel=1
ConnectFunctions=YYY
Description=Snowflake DSII
Driver=$CONDA_PREFIX/opt/snowflake_odbc/lib/libSnowflake.so
DriverODBCVer=03.52
SQLLevel=1
ODBCINST_INI
    echo -e "${GREEN}set ODBCSYSINI variable as conda env var${ENDCOLOR}"
    conda env config vars set ODBCSYSINI=$CONDA_PREFIX/etc
    eval "$(conda shell.bash hook)"
    conda activate $CONDA_DEFAULT_ENV
    export ODBCSYSINI=$CONDA_PREFIX/etc
    echo -e "${GREEN}cleanup temporary files${ENDCOLOR}"
    rm -f snowflake_linux_x8664_odbc-2.23.2.tgz
    echo -e "${GREEN}Installation complete!${ENDCOLOR}"
else
    echo -e "${RED}This script works inside conda environment please create a conda environment and try again${ENDCOLOR}"
fi
