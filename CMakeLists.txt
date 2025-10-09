cmake_minimum_required(VERSION 3.16)
project(async VERSION 1.0.0 LANGUAGES CXX)

if(POLICY CMP0167)
  cmake_policy(SET CMP0167 OLD)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_library(async SHARED async.cpp async.h)
set_target_properties(async PROPERTIES
    OUTPUT_NAME "async"
    SOVERSION 1
    VERSION ${PROJECT_VERSION}
    WINDOWS_EXPORT_ALL_SYMBOLS ON
)
find_package(Threads REQUIRED)
target_link_libraries(async PRIVATE Threads::Threads $<$<PLATFORM_ID:Windows>:ws2_32;mswsock>)

add_executable(bulk main.cpp)
target_link_libraries(bulk PRIVATE async)

include(GNUInstallDirs)

install(TARGETS async
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    COMPONENT async_lib
)
install(FILES async.h DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/async COMPONENT async_dev)

install(TARGETS bulk
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    COMPONENT async_demo
)

if(WIN32)
  install(TARGETS async
      RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
      COMPONENT async_lib
  )

  find_program(_CXX ${CMAKE_CXX_COMPILER})
  get_filename_component(_MINGW_BINDIR "${_CXX}" DIRECTORY)

  set(_RUNTIME_DLLS
    libgcc_s_seh-1.dll
    libstdc++-6.dll
    libwinpthread-1.dll
  )

  foreach(_dll IN LISTS _RUNTIME_DLLS)
    if(EXISTS "${_MINGW_BINDIR}/${_dll}")
      install(FILES "${_MINGW_BINDIR}/${_dll}" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT async_lib)
    endif()
  endforeach()
endif()

set(CPACK_PACKAGE_NAME "async")
set(CPACK_PACKAGE_VENDOR "your-org")
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_GENERATOR "TGZ;ZIP")
set(CPACK_COMPONENTS_ALL async_lib async_dev async_demo)
set(CPACK_COMPONENT_ASYNC_LIB_DESCRIPTION "Shared library libasync.so/async.dll")
set(CPACK_COMPONENT_ASYNC_DEV_DESCRIPTION "Headers for async library")
set(CPACK_COMPONENT_ASYNC_DEMO_DESCRIPTION "Demo executable bulk")
include(CPack)
