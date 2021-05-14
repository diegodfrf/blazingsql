#!/bin/bash
#
# This script install BlazingSQL dependencies based on rapids version
#
set -e

NUMARGS=$#
ARGS=$*

VALIDARGS="rapids cuda -h nightly e2e mysql postgres snowflake"

HELP="$0 [-h] rapids=[rapids version] cuda=[cuda version]
  e2e         - Install e2e basic dependencies for e2e tests
  -h          - Print help text
  mysql       - Install mysql dependencies for e2e tests
  nightly     - Install nightly dependencies
  postgres    - Install postgres dependencies for e2e tests
  snowflake   - Install snowflake dependencies for e2e tests
"

export GREEN='\033[0;32m'
export RED='\033[0;31m'
BOLDGREEN="\e[1;${GREEN}"
ITALICRED="\e[3;${RED}"
ENDCOLOR="\e[0m"

RAPIDS_VERSION=""
CUDA_VERSION=""
CHANNEL=""

function hasArg {
    (( ${NUMARGS} != 0 )) && (echo " ${ARGS} " | grep -q " $1 ")
}

if hasArg -h; then
    echo "${HELP}"
    exit 0
fi

# Check for valid usage
if (( ${NUMARGS} != 0 )); then
    for a in ${ARGS}; do
      if [[ $a == "rapids="* ]]; then
          RAPIDS_VERSION=${a#"rapids="}
          if [ -z $RAPIDS_VERSION ] || [ $RAPIDS_VERSION == $a ] ; then
              echo "Invalid option: ${a}"
              exit 1
          fi
      fi
      if [[ $a == "cuda="* ]]; then
          CUDA_VERSION=${a#"cuda="}
          if [ -z $CUDA_VERSION ] || [ $CUDA_VERSION == $a ] ; then
              echo "Invalid option: ${a}"
              exit 1
          fi
      fi
      a=${a%=*}
      if ! (echo " ${VALIDARGS} " | grep -q " ${a} "); then
          echo "Invalid option: ${a}"
          exit 1
      fi
    done
fi

if hasArg nightly; then
  CHANNEL="-nightly"
fi

if [ "$RAPIDS_VERSION" ] && [ "$CUDA_VERSION" ]; then
  echo -e "${GREEN}Installing dependencies${ENDCOLOR}"
  conda install --yes -c conda-forge spdlog'>=1.7.0,<2.0.0a0' google-cloud-cpp=1.25 ninja mysql-connector-cpp=8.0.23 libpq=13 nlohmann_json=3.9.1 unixodbc=2.3.9
  # NOTE cython must be the same of cudf (for 0.11 and 0.12 cython is >=0.29,<0.30)
  conda install --yes -c conda-forge cmake=3.18 gtest==1.10.0=h0efe328_4 gmock cppzmq cython=0.29 openjdk=8.0 maven jpype1 netifaces pyhive pytest tqdm ipywidgets boost-cpp=1.76.0

  echo -e "${GREEN}Install RAPIDS dependencies${ENDCOLOR}"
  conda install --yes -c rapidsai$CHANNEL -c nvidia -c conda-forge -c defaults dask-cuda=$RAPIDS_VERSION dask-cudf=$RAPIDS_VERSION cudf=$RAPIDS_VERSION ucx-py=$RAPIDS_VERSION ucx-proc=*=gpu cudatoolkit=$CUDA_VERSION
fi

if hasArg e2e; then
  echo -e "${GREEN}Install E2E test dependencies${ENDCOLOR}"
  conda install --yes -c conda-forge openjdk=8.0 maven pyspark=3.0.0 pytest
  pip install pydrill openpyxl pymysql gitpython pynvml gspread oauth2client docker
fi

if hasArg mysql; then
  conda install --yes -c conda-forge mysql-connector-python
fi

if hasArg postgres; then
  conda install --yes -c conda-forge psycopg2
fi

if hasArg snowflake; then
  pip install snowflake-connector-python==2.4.2
fi
if [ $? -eq 0 ]; then
  echo -e "${GREEN}Installation complete${ENDCOLOR}"
else
  echo -e "${RED}Installation failed${ENDCOLOR}"
fi
