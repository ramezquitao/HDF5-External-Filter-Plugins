cmake_minimum_required(VERSION 3.18)
project(lzo_plugin)
#=============================================================================
# do some plugin specific configuration
#=============================================================================
#find_package(LZO2 REQUIRED)
find_package(HDF5 REQUIRED COMPONENTS C) 

#=============================================================================
# setup the plugin name
#=============================================================================
set(PLUGIN_NAME h5lzo)

#=============================================================================
# adding required subdirectories
#=============================================================================
add_subdirectory(src)
#add_subdirectory(test)
