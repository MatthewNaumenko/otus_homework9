cmake_minimum_required(VERSION 3.16)
project(async VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# libasync.so
add_library(async SHARED async.cpp async.h)
set_target_properties(async PROPERTIES
    OUTPUT_NAME "async"       
    SOVERSION 1
    VERSION ${PROJECT_VERSION}
    WINDOWS_EXPORT_ALL_SYMBOLS ON
)

find_package(Threads REQUIRED)
target_link_libraries(async PRIVATE Threads::Threads)

add_executable(async_cli main.cpp)
target_link_libraries(async_cli PRIVATE async)

include(GNUInstallDirs)

install(TARGETS async
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}     
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

install(FILES async.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/async
)

install(TARGETS async_cli
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} 
)

