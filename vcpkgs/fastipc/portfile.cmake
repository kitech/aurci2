set(VCPKG_BUILD_TYPE release)
set(VCPKG_POLICY_ONLY_RELEASE_CRT enabled)
set(VCPKG_POLICY_MISMATCHED_NUMBER_OF_BINARIES enabled)

# This port builds FastIPC from the upstream v1.0.0 tag with the Zig build
# system; it does not use the prebuilt release archives.
#
# SHA512 of the v1.0.0 tag tarball downloaded through the gh-proxy GITHUB_HOST
# below. To refresh it, temporarily set this to 0 and let vcpkg print the hash.
set(_FASTIPC_SOURCE_SHA512 "7b9cf1603fbcec75da67773bca837317b897fb1767a47dc0c6767a5768c76ed40cb6f57d227c9af1adbb0d1c0af42b10da5a1a237d462860b48889ea9ee17724")

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO fastipc/fastipc
    REF v1.0.0
    SHA512 "${_FASTIPC_SOURCE_SHA512}"
    HEAD_REF main
    GITHUB_HOST "https://gh-proxy.com/https://github.com"
)

# This overlay port requires Zig 0.17.0 or newer on PATH.
# FastIPC's build.zig.zon declares minimum_zig_version = "0.17.0".
find_program(ZIG_EXECUTABLE NAMES zig REQUIRED)

execute_process(
    COMMAND "${ZIG_EXECUTABLE}" version
    OUTPUT_VARIABLE _zig_version
    OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE _zig_result
)
if(NOT _zig_result EQUAL 0)
    message(FATAL_ERROR "Failed to run zig: ${ZIG_EXECUTABLE}")
endif()
if(_zig_version VERSION_LESS "0.17.0")
    message(FATAL_ERROR "FastIPC requires Zig >= 0.17.0, found ${_zig_version}")
endif()

# Keep every Zig cache and temporary install prefix inside vcpkg's build tree.
# They are all removed after the package files have been installed.
set(_zig_prefix "${CURRENT_BUILDTREES_DIR}/${TARGET_TRIPLET}-zig-build")
set(_zig_cache_dir "${CURRENT_BUILDTREES_DIR}/${TARGET_TRIPLET}-zig-cache")
set(_zig_global_cache_dir "${CURRENT_BUILDTREES_DIR}/${TARGET_TRIPLET}-zig-global-cache")
set(_zig_config_dir "${CURRENT_BUILDTREES_DIR}/${TARGET_TRIPLET}-zig-config")

file(REMOVE_RECURSE
    "${_zig_prefix}"
    "${_zig_cache_dir}"
    "${_zig_global_cache_dir}"
    "${_zig_config_dir}"
)
file(MAKE_DIRECTORY "${_zig_config_dir}")

# Zig 0.17 no longer accepts --global-cache-dir for `zig build`; it uses
# ZIG_GLOBAL_CACHE_DIR instead. Save and restore any caller-provided value.
set(_zig_global_cache_env_backup "$ENV{ZIG_GLOBAL_CACHE_DIR}")
set(ENV{ZIG_GLOBAL_CACHE_DIR} "${_zig_global_cache_dir}")

if(VCPKG_TARGET_IS_OSX AND VCPKG_TARGET_ARCHITECTURE STREQUAL "x64")
    # FastIPC upstream does not officially support Intel macOS. This branch
    # makes x64-osx and x64-osx-dynamic installable by cross-building the
    # genuine x86_64-macos Mach-O library with Zig.
    message(WARNING
        "FastIPC upstream does not officially support Intel macOS. "
        "Building x86_64-macos only to satisfy x64-osx triplets.")

    vcpkg_execute_required_process(
        COMMAND
            "${ZIG_EXECUTABLE}"
            build
            --cache-dir "${_zig_cache_dir}"
            --prefix "${_zig_prefix}"
            -Dtarget=x86_64-macos
            -Doptimize=ReleaseFast
        WORKING_DIRECTORY "${SOURCE_PATH}"
        LOGNAME "build-fastipc-x64-osx-${TARGET_TRIPLET}"
    )

    set(_fastipc_header_dir "${_zig_prefix}/include")

    file(INSTALL
        "${_zig_prefix}/lib/libfastipc.dylib"
        DESTINATION "${CURRENT_PACKAGES_DIR}/lib"
    )

elseif(VCPKG_CMAKE_SYSTEM_NAME STREQUAL "Android")
    # FastIPC upstream does not list Android as supported. This port exposes
    # an experimental arm64-android build against the NDK selected by vcpkg.
    if(NOT VCPKG_TARGET_ARCHITECTURE STREQUAL "arm64")
        message(FATAL_ERROR "FastIPC Android support in this port is arm64 only")
    endif()

    # Android support in vcpkg is driven by ANDROID_NDK_HOME. Do not invent a
    # new triplet variable; use the same NDK that vcpkg's android toolchain
    # uses, so Zig's libc is exactly that NDK's libc.
    set(_android_ndk "$ENV{ANDROID_NDK_HOME}")
    if(NOT _android_ndk OR NOT EXISTS "${_android_ndk}/build/cmake/android.toolchain.cmake")
        message(FATAL_ERROR
            "FastIPC Android builds require ANDROID_NDK_HOME pointing to an "
            "Android NDK with build/cmake/android.toolchain.cmake.")
    endif()

    set(_android_api_level "${VCPKG_CMAKE_SYSTEM_VERSION}")
    if(_android_api_level MATCHES "^android-([0-9]+)$")
        set(_android_api_level "${CMAKE_MATCH_1}")
    endif()
    if(NOT _android_api_level MATCHES "^[0-9]+$")
        set(_android_api_level "28")
    endif()

    if(CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux")
        set(_ndk_host "linux-x86_64")
    elseif(CMAKE_HOST_SYSTEM_NAME STREQUAL "Darwin")
        set(_ndk_host "darwin-x86_64")
    elseif(CMAKE_HOST_SYSTEM_NAME STREQUAL "Windows")
        set(_ndk_host "windows-x86_64")
    else()
        message(FATAL_ERROR "Unsupported host system for Android NDK: ${CMAKE_HOST_SYSTEM_NAME}")
    endif()

    set(_android_triple "aarch64-linux-android")
    set(_android_prebuilt "${_android_ndk}/toolchains/llvm/prebuilt/${_ndk_host}")
    set(_android_sysroot "${_android_prebuilt}/sysroot")
    set(_android_include "${_android_sysroot}/usr/include")
    set(_android_sys_include "${_android_sysroot}/usr/include/${_android_triple}")
    if(NOT IS_DIRECTORY "${_android_sys_include}")
        set(_android_sys_include "${_android_include}")
    endif()
    set(_android_lib_root "${_android_sysroot}/usr/lib/${_android_triple}")
    set(_android_crt_dir "${_android_lib_root}/${_android_api_level}")

    if(NOT IS_DIRECTORY "${_android_crt_dir}")
        message(FATAL_ERROR
            "Android NDK sysroot is missing ${_android_crt_dir}. "
            "Check ANDROID_NDK_HOME and VCPKG_CMAKE_SYSTEM_VERSION.")
    endif()

    # Zig 0.17 has no bundled Bionic libc. Generate the LibCInstallation file
    # from the vcpkg-selected NDK and pass it with --libc. All fields must be
    # present even when they are empty for Android.
    set(_android_libc_file "${_zig_config_dir}/android-libc.txt")
    file(WRITE "${_android_libc_file}"
        "include_dir=${_android_include}\n"
        "sys_include_dir=${_android_sys_include}\n"
        "crt_dir=${_android_crt_dir}\n"
        "msvc_lib_dir=\n"
        "kernel32_lib_dir=\n"
        "cc_dir=${_android_crt_dir}\n"
        "darwin_sdk_dir=\n"
    )

    message(STATUS "FastIPC Android NDK: ${_android_ndk}")
    message(STATUS "FastIPC Android sysroot: ${_android_sysroot}")
    message(STATUS "FastIPC Android libc file: ${_android_libc_file}")

    message(WARNING
        "FastIPC upstream does not officially list Android support. "
        "Building experimental arm64-android library with the vcpkg NDK libc.")

    vcpkg_execute_required_process(
        COMMAND
            "${ZIG_EXECUTABLE}"
            build
            --cache-dir "${_zig_cache_dir}"
            --prefix "${_zig_prefix}"
            --sysroot "${_android_sysroot}"
            --libc "${_android_libc_file}"
            -Dtarget=${_android_triple}.${_android_api_level}
            -Doptimize=ReleaseFast
        WORKING_DIRECTORY "${SOURCE_PATH}"
        LOGNAME "build-fastipc-android-${TARGET_TRIPLET}"
    )

    set(_fastipc_header_dir "${_zig_prefix}/include")

    file(INSTALL
        "${_zig_prefix}/lib/libfastipc.so"
        DESTINATION "${CURRENT_PACKAGES_DIR}/lib"
    )

else()
    # zig build dist builds the upstream four packaged targets with the
    # project's own CPU/OS floors: linux-x64, linux-arm64, win-x64, osx-arm64.
    vcpkg_execute_required_process(
        COMMAND
            "${ZIG_EXECUTABLE}"
            build
            --cache-dir "${_zig_cache_dir}"
            --prefix "${_zig_prefix}"
            -Doptimize=ReleaseFast
            dist
        WORKING_DIRECTORY "${SOURCE_PATH}"
        LOGNAME "build-fastipc-${TARGET_TRIPLET}"
    )

    if(VCPKG_TARGET_IS_WINDOWS)
        set(_fastipc_rid "win-x64")
    elseif(VCPKG_TARGET_IS_OSX)
        set(_fastipc_rid "osx-arm64")
    elseif(VCPKG_TARGET_ARCHITECTURE STREQUAL "arm64")
        set(_fastipc_rid "linux-arm64")
    else()
        set(_fastipc_rid "linux-x64")
    endif()

    set(_fastipc_native "${_zig_prefix}/dist/runtimes/${_fastipc_rid}/native")
    set(_fastipc_header_dir "${_zig_prefix}/dist/include")

    if(VCPKG_TARGET_IS_WINDOWS)
        file(INSTALL
            "${_fastipc_native}/fastipc.lib"
            DESTINATION "${CURRENT_PACKAGES_DIR}/lib"
        )
        file(INSTALL
            "${_fastipc_native}/fastipc.dll"
            DESTINATION "${CURRENT_PACKAGES_DIR}/bin"
        )
    elseif(VCPKG_TARGET_IS_OSX)
        file(INSTALL
            "${_fastipc_native}/libfastipc.dylib"
            DESTINATION "${CURRENT_PACKAGES_DIR}/lib"
        )
    else()
        file(INSTALL
            "${_fastipc_native}/libfastipc.so"
            DESTINATION "${CURRENT_PACKAGES_DIR}/lib"
        )
    endif()
endif()

file(INSTALL
    "${_fastipc_header_dir}/fipc.h"
    "${_fastipc_header_dir}/fipc.hpp"
    DESTINATION "${CURRENT_PACKAGES_DIR}/include"
)

file(INSTALL
    "${CMAKE_CURRENT_LIST_DIR}/unofficial-fastipc-config.cmake"
    DESTINATION "${CURRENT_PACKAGES_DIR}/share/unofficial-fastipc"
    RENAME "unofficial-fastipc-config.cmake"
)

file(INSTALL
    "${CMAKE_CURRENT_LIST_DIR}/usage"
    DESTINATION "${CURRENT_PACKAGES_DIR}/share/unofficial-fastipc"
)

vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/LICENSE")

# Zig must not leave any cache, config or temporary prefix behind.
file(REMOVE_RECURSE
    "${_zig_prefix}"
    "${_zig_cache_dir}"
    "${_zig_global_cache_dir}"
    "${_zig_config_dir}"
)

if(_zig_global_cache_env_backup STREQUAL "")
    unset(ENV{ZIG_GLOBAL_CACHE_DIR})
else()
    set(ENV{ZIG_GLOBAL_CACHE_DIR} "${_zig_global_cache_env_backup}")
endif()
