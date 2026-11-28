get_filename_component(_FASTIPC_PREFIX "${CMAKE_CURRENT_LIST_DIR}/../.." ABSOLUTE)

if(NOT TARGET unofficial::fastipc::fastipc)
    add_library(unofficial::fastipc::fastipc SHARED IMPORTED)

    set_target_properties(
        unofficial::fastipc::fastipc
        PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${_FASTIPC_PREFIX}/include"
    )

    if(WIN32)
        set_target_properties(
            unofficial::fastipc::fastipc
            PROPERTIES
            IMPORTED_IMPLIB "${_FASTIPC_PREFIX}/lib/fastipc.lib"
            IMPORTED_LOCATION "${_FASTIPC_PREFIX}/bin/fastipc.dll"
            INTERFACE_COMPILE_DEFINITIONS "FASTIPC_SHARED=1"
        )
    elseif(APPLE)
        set_target_properties(
            unofficial::fastipc::fastipc
            PROPERTIES
            IMPORTED_LOCATION "${_FASTIPC_PREFIX}/lib/libfastipc.dylib"
        )
    else()
        set_target_properties(
            unofficial::fastipc::fastipc
            PROPERTIES
            IMPORTED_LOCATION "${_FASTIPC_PREFIX}/lib/libfastipc.so"
        )
    endif()
endif()
