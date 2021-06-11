#!/bin/bash

OPTIONS=t:c:vkhb:
LONGOPTS=io,libengine,algebra,pyblazing,e2e,tests:,config-file:,verbose,skipe2e,help,branch-testing-files:

! PARSED=$(getopt --options=$OPTIONS --longoptions=$LONGOPTS --name "$0" -- "$@")
if [[ ${PIPESTATUS[0]} -ne 0 ]]; then
    exit 2
fi

eval set -- "$PARSED"

io=No
libengine=No
algebra=No
pyblazing=No
e2e=No
tests=""
configfile=""
verbose=No
skipe2e=No
printhelp=No
branch_testing_files="master"
testAll=true

while true; do
    case "$1" in
        --io)
            io=Yes
            testAll=false
            shift
            ;;
        --libengine)
            libengine=Yes
            testAll=false
            shift
            ;;
        --algebra)
            algebra=Yes
            testAll=false
            shift
            ;;
        --pyblazing)
            pyblazing=Yes
            testAll=false
            shift
            ;;
        --e2e)
            e2e=Yes
            testAll=false
            shift
            ;;
        -t|--tests)
            tests="$2"
            shift 2
            ;;
        -c|--config-file)
            configfile="$2"
            shift 2
            ;;
        -v|--verbose)
            verbose=Yes
            shift
            ;;
        -k|--skipe2e)
            skipe2e=Yes
            shift
            ;;
        -h|--help)
            printhelp=Yes
            shift
            ;;
        -b|--branch-testing-files)
          branch_testing_files="$2"
          shift 2
          ;;
        --)
            shift
            break
            ;;
        *)
            echo "Programming error"
            exit 3
            ;;
    esac
done

if [ -n $CONDA_PREFIX ]; then
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$CONDA_PREFIX/lib:$CONDA_PREFIX/lib64
fi

# Logger function for build status output
function logger() {
  echo -e "\n>>>> $@\n"
}

HELP="Voltron testing
Tool to run Unit Test and End-to-End tests.

Usage:
   test.sh                          Default action (no args) is to test all code and packages
   test.sh [options]

Options:
   --io                             Test the IO C++ code only
   --libengine                      Test the engine C++ code only
   --algebra                        Test the algebra package
   --pyblazing                      Test the pyblazing interface
   --e2e                            Test the end to end tests
   -k, --skipe2e                    Skip end to end tests (force not run 'e2e' tests)
   -v, --verbose                    Verbose test mode
   -h, --help                       Print help
   -t=<name>, --tests=<name>        Optional argument to use after 'e2e' run specific e2e test groups.
                                    The comma separated values are the e2e tests to run, where
                                    each value is the python filename of the test located in
                                    blazingsql/tests/BlazingSQLTest/EndToEndTests/ in TestSuites/ and oldScripts/
                                    (e.g. 'castSuite, groupBySuite' or 'literalSuite' for single test).
                                    Empty means will run all the e2e tests)
   -c <file>, --config-file <file>  Set configuration file for E2E tests, the file must be located
                                    in blazingsql/tests/BlazingSQLTest/Configuration/
"

# TODO william kharoly felipe we should try to enable and use this param in the future (compare result from spreadsheet)
#    -c           - save and use a cache of the previous e2e test runs from
#                   Google Docs. The cache (e2e-gspread-cache.parquet) will be
#                   located inside the env BLAZINGSQL_E2E_LOG_DIRECTORY (which
#                   is usually pointing to the CONDA_PREFIX dir)

# Set defaults for vars modified by flags to this script
#GSPREAD_CACHE="false"

if [ "$printhelp" == "Yes" ]; then
  echo "${HELP}"
  exit 0
fi

# NOTE if WORKSPACE is not defined we assume the user is in the blazingsql project root folder
if [ -z $WORKSPACE ] ; then
    logger "WORKSPACE is not defined, it should point to the blazingsql project root folder"
    logger "Using $PWD as WORKSPACE"
    WORKSPACE=$PWD
fi

# TODO william kharoly felipe we should try to enable and use this param in the future (compare result from spreadsheet)
#if hasArg -c; then
#    GSPREAD_CACHE="true"
#fi

function get_blazingtesting_files {
  echo "clone repository asdlkjsadlkjsadlkjsalkdj"
  if [ -z $BLAZINGSQL_E2E_DATA_DIRECTORY ] || [ -z $BLAZINGSQL_E2E_FILE_RESULT_DIRECTORY ]; then
      if [ -d $CONDA_PREFIX/blazingsql-testing-files/data/ ]; then
          logger "Using $CONDA_PREFIX/blazingsql-testing-files folder for end to end tests..."
          cd $CONDA_PREFIX
          cd blazingsql-testing-files
          git pull
      else
          set +e
          logger "Preparing $CONDA_PREFIX/blazingsql-testing-files folder for end to end tests..."
          cd $CONDA_PREFIX

          # Only for PRs
          PR_BR="blazingdb:"${TARGET_BRANCH}
          if [ ! -z "${PR_AUTHOR}" ]; then
              echo "PR_AUTHOR: "${PR_AUTHOR}
              git clone --depth 1 https://github.com/${PR_AUTHOR}/blazingsql-testing-files.git --branch ${SOURCE_BRANCH} --single-branch
              # if branch exits
              if [ $? -eq 0 ]; then
                  echo "The fork exists"
                  PR_BR=${PR_AUTHOR}":"${SOURCE_BRANCH}
              else
                  echo "The fork doesn't exist"
                  git clone --depth 1 https://github.com/BlazingDB/blazingsql-testing-files.git --branch ${TARGET_BRANCH} --single-branch
              fi
          else
              echo "PR_AUTHOR not found cloning blazingsql testing files from $branch_testing_files branch"
              git clone --depth 1 https://github.com/BlazingDB/blazingsql-testing-files.git --branch $branch_testing_files --single-branch
          fi
          set -e

          echo "Cloned from "${PR_BR}
          cd blazingsql-testing-files/data
          tar xf tpch.tar.gz
          tar xf tpch-with-nulls.tar.gz
          tar xf tpch-json.tar.gz -C .
          tar xf smiles.tar.gz
          logger "$CONDA_PREFIX/blazingsql-testing-files folder for end to end tests... ready!"
      fi
      export BLAZINGSQL_E2E_DATA_DIRECTORY=$CONDA_PREFIX/blazingsql-testing-files/data/
      export BLAZINGSQL_E2E_FILE_RESULT_DIRECTORY=$CONDA_PREFIX/blazingsql-testing-files/results/
  else
      blazingsql_testing_files_dir=$(realpath $BLAZINGSQL_E2E_FILE_RESULT_DIRECTORY/../)
      logger "Using $blazingsql_testing_files_dir folder for end to end tests..."
      cd $blazingsql_testing_files_dir
      git pull
  fi
}

################################################################################
if $testAll || [ "$io" == "Yes" ]; then
    logger "Running IO Unit tests..."
    cd ${WORKSPACE}/io/build
    SECONDS=0
    if [ "$verbose" == "Yes" ]; then
      ctest --verbose
    else
      ctest
    fi
    duration=$SECONDS
    echo "Total time for IO Unit tests: $(($duration / 60)) minutes and $(($duration % 60)) seconds"
fi

if $testAll || [ "$libengine" == "Yes" ]; then
    logger "Running Engine Unit tests..."
    get_blazingtesting_files

    cd ${WORKSPACE}/engine/build
    SECONDS=0
    if [ "$verbose" == "Yes" ]; then
      ctest --verbose
    else
      ctest
    fi
    duration=$SECONDS
    echo "Total time for Engine Unit tests: $(($duration / 60)) minutes and $(($duration % 60)) seconds"
fi

if $testAll || [ "$algebra" == "Yes" ]; then
    # TODO mario
    echo "TODO"
fi

if $testAll || [ "$pyblazing" == "Yes" ]; then
    logger "Running Pyblazing Unit tests..."
    SECONDS=0
    cd ${WORKSPACE}/pyblazing/tests
    if [ "$verbose" == "Yes" ]; then
      pytest --verbose
    else
      pytest
    fi
    duration=$SECONDS
    echo "Total time for Pyblazing Unit tests: $(($duration / 60)) minutes and $(($duration % 60)) seconds"
fi

if [ "$skipe2e" == "Yes" ]; then
    logger "Skipping end to end tests..."
else
    if $testAll || [ "$e2e" == "Yes" ]; then
        get_blazingtesting_files

        # TODO william kharoly felipe we should try to enable and use this param in the future (compare result from spreadsheet)
        #export BLAZINGSQL_E2E_GSPREAD_CACHE=$GSPREAD_CACHE

        export BLAZINGSQL_E2E_TARGET_TEST_GROUPS=$tests

        cd ${WORKSPACE}/tests/BlazingSQLTest/

        if [ "$configfile" != "" ]; then
          echo "Loading config file: ${configfile}"
          python -m EndToEndTests.mainE2ETests --config-file $configfile
        else
          if [ "$BLAZINGSQL_E2E_IN_GPUCI_ENV" == "true" ] ; then
            python -m EndToEndTests.mainE2ETests --config-file config_singlenode.yaml
            python -m EndToEndTests.mainE2ETests --config-file config_multinode.yaml
          else
            python -m EndToEndTests.mainE2ETests --config-file config.yaml
          fi
        fi
    fi
fi
