#=============================================================================
# Copyright 2021 VoltronData, Inc.
#     Copyright 2021 Percy Camilo Triveño Aucahuasi <percy.camilo.ta@gmail.com>
#=============================================================================

# BEGIN UCX
set(UCX_INSTALL_DIR $ENV{CONDA_PREFIX})
set(UCX_INCLUDE_DIR $ENV{CONDA_PREFIX}/include)
find_package(UCX REQUIRED)
# END UCX
