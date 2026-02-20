cmake_minimum_required(VERSION 3.23)

project(
    csv_report_calc
    VERSION 1.0.0
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ──────────────────────────────────────────────
# Compiler warnings
# ──────────────────────────────────────────────
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(
        -Wall
        -Wextra
        -Wpedantic
        -Wconversion
        -Wshadow
    )
endif()

# ──────────────────────────────────────────────
# Dependencies via FetchContent
# ──────────────────────────────────────────────
include(FetchContent)

# Boost (program_options + accumulators)
find_package(Boost 1.83 QUIET COMPONENTS program_options)
if(NOT Boost_FOUND)
    message(STATUS "Boost not found locally, fetching...")
    FetchContent_Declare(
        boost
        URL https://github.com/boostorg/boost/releases/download/boost-1.84.0/boost-1.84.0.tar.xz
        URL_HASH SHA256=2e64e5d79a738d0fa6fb546c6e5c2bd28f88d268a2a080546f74e5ff98f29d0e
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
    set(BOOST_INCLUDE_LIBRARIES program_options accumulators)
    set(BOOST_ENABLE_CMAKE ON)
    FetchContent_MakeAvailable(boost)
endif()

# toml++
FetchContent_Declare(
    tomlplusplus
    GIT_REPOSITORY https://github.com/marzer/tomlplusplus.git
    GIT_TAG        v3.4.0
)
FetchContent_MakeAvailable(tomlplusplus)

# spdlog
FetchContent_Declare(
    spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG        v1.17.0
)
FetchContent_MakeAvailable(spdlog)

# Catch2 (для unit-тестов)
FetchContent_Declare(
    Catch2
    GIT_REPOSITORY https://github.com/catchorg/Catch2.git
    GIT_TAG        v3.13.0
)
FetchContent_MakeAvailable(Catch2)

# ──────────────────────────────────────────────
# Основной исполняемый файл
# ──────────────────────────────────────────────
add_executable(${PROJECT_NAME}
    src/main.cpp
)

target_include_directories(${PROJECT_NAME} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(${PROJECT_NAME} PRIVATE
    Boost::program_options
    tomlplusplus::tomlplusplus
    spdlog::spdlog
)

# ──────────────────────────────────────────────
# Unit-тесты
# ──────────────────────────────────────────────
enable_testing()

add_executable(tests
    tests/test_median.cpp
)

target_include_directories(tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(tests PRIVATE
    Catch2::Catch2WithMain
    Boost::program_options
    tomlplusplus::tomlplusplus
    spdlog::spdlog
)

include(CTest)
include(Catch)
catch_discover_tests(tests)
