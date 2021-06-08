from Configuration import Settings
from Configuration import ExecutionMode

from EndToEndTests.oldScripts import hiveFileTest
from EndToEndTests.oldScripts import unsignedTypeTest
from EndToEndTests.oldScripts import columnBasisTest
from EndToEndTests.oldScripts import dateTest
from EndToEndTests.oldScripts import fileSystemHdfsTest
from EndToEndTests.oldScripts import fileSystemS3Test
from EndToEndTests.oldScripts import fileSystemGSTest
from EndToEndTests.oldScripts import loggingTest
from EndToEndTests.oldScripts import smilesTest
from EndToEndTests.oldScripts import configOptionsTest
from EndToEndTests.oldScripts import tablesFromSQL

def runLegacyTest(bc, dask_client, drill, spark):
    targetTestGroups = Settings.data["RunSettings"]["targetTestGroups"]

    nRals = Settings.data["RunSettings"]["nRals"]
    dir_data_file = Settings.data["TestSettings"]["dataDirectory"]
    testsWithNulls = Settings.data["RunSettings"]["testsWithNulls"]

    runAllTests = (
        len(targetTestGroups) == 0
    )  # if targetTestGroups was empty the user wants to run all the tests

    if runAllTests or ("hiveFileSuite" in targetTestGroups):
        hiveFileTest.main(dask_client, spark, dir_data_file, bc, nRals)

    if runAllTests or ("unsignedTypeSuite" in targetTestGroups):
        unsignedTypeTest.main(dask_client, drill, dir_data_file, bc, nRals)

    if runAllTests or ("columnBasisSuite" in targetTestGroups):
        columnBasisTest.main(dask_client, drill, dir_data_file, bc, nRals)

    if runAllTests or ("dateSuite" in targetTestGroups):
        dateTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)

    # HDFS is not working yet
    # fileSystemHdfsTest.main(dask_client, drill, dir_data_file, bc)

    # HDFS is not working yet
    # mixedFileSystemTest.main(dask_client, drill, dir_data_file, bc)

    if not testsWithNulls:
        if Settings.execution_mode != ExecutionMode.GPUCI:
            if runAllTests or ("fileSystemS3Suite" in targetTestGroups):
                fileSystemS3Test.main(dask_client, drill, dir_data_file, bc, nRals)

            if runAllTests or ("fileSystemGSSuite" in targetTestGroups):
                fileSystemGSTest.main(dask_client, drill, dir_data_file, bc, nRals)

    if runAllTests or ("LoggingSuite" in targetTestGroups):
        loggingTest.main(dask_client, dir_data_file, bc, nRals)

    # TODO re enable this test once we have the new version of dask
    # https://github.com/dask/distributed/issues/4645
    # https://github.com/rapidsai/cudf/issues/7773
    # if runAllTests or ("smilesSuite" in targetTestGroups):
    #    smilesTest.main(dask_client, spark, dir_data_file, bc, nRals)

    if testsWithNulls == "true":
        if Settings.execution_mode != ExecutionMode.GPUCI:
            if runAllTests or ("tablesFromSQL" in targetTestGroups):
                tablesFromSQL.main(dask_client, drill, spark, dir_data_file, bc, nRals)

    # WARNING!!! This Test must be the last one to test ----------------------------------------------------------------
    if runAllTests or ("configOptionsSuite" in targetTestGroups):
        configOptionsTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)