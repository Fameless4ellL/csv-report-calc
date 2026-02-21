/**
 * \file test_parser.cpp
 * \brief Unit-тесты для config_parser
 */

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

#include "parser.hpp"

using csv_median::config_parser;
using csv_median::app_config;

namespace fs = std::filesystem;

struct temp_toml {
    fs::path path;

    explicit temp_toml(const std::string& content_) {
        const auto name = "config_test_"
            + std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()
            ) + ".toml";
        path = fs::temp_directory_path() / name;
        std::ofstream f{ path };
        f << content_;
    }

    ~temp_toml() {
        fs::remove(path);
    }

    // Путь в виде строки — удобно для argv
    std::string str() const { return path.string(); }
};

struct fake_argv {
    std::vector<std::string>    storage;
    std::vector<const char*>    ptrs;

    explicit fake_argv(std::vector<std::string> args_)
        : storage{ std::move(args_) }
    {
        for (const auto& s : storage) {
            ptrs.push_back(s.c_str());
        }
    }

    int argc() const { return static_cast<int>(ptrs.size()); }
    const char* const* argv() const { return ptrs.data(); }
};

TEST_CASE("config - full valid config", "[config]") {
    temp_toml cfg{
        "[main]\n"
        "input = '/data/input'\n"
        "output = '/data/output'\n"
        "filename_mask = ['trade', 'level']\n"
    };

    fake_argv args{ {"app", "--config", cfg.str()} };
    config_parser parser;
    auto [config, err] = parser.parse(args.argc(), args.argv());

    REQUIRE_FALSE(err);
    CHECK(config.input_dir == fs::path{ "/data/input" });
    CHECK(config.output_dir == fs::path{ "/data/output" });
    REQUIRE(config.filename_masks.size() == 2);
    CHECK(config.filename_masks[0] == "trade");
    CHECK(config.filename_masks[1] == "level");
}

TEST_CASE("config - only required input field", "[config]") {
    temp_toml cfg{
        "[main]\n"
        "input = '/data/input'\n"
    };

    fake_argv args{ {"app", "--config", cfg.str()} };
    config_parser parser;
    auto [config, err] = parser.parse(args.argc(), args.argv());

    REQUIRE_FALSE(err);
    CHECK(config.input_dir == fs::path{ "/data/input" });
    CHECK(config.output_dir == fs::current_path() / "output");
    CHECK(config.filename_masks.empty());
}

TEST_CASE("config - empty filename_mask means all files", "[config]") {
    temp_toml cfg{
        "[main]\n"
        "input = './data'\n"
        "filename_mask = []\n"
    };

    fake_argv args{ {"app", "--config", cfg.str()} };
    config_parser parser;
    auto [config, err] = parser.parse(args.argc(), args.argv());

    REQUIRE_FALSE(err);
    CHECK(config.filename_masks.empty());
}

TEST_CASE("config - cfg alias works", "[config]") {
    temp_toml cfg{
        "[main]\n"
        "input = './data'\n"
    };

    fake_argv args{ {"app", "--cfg", cfg.str()} };
    config_parser parser;
    auto [config, err] = parser.parse(args.argc(), args.argv());

    REQUIRE_FALSE(err);
    CHECK(config.input_dir == fs::path{ "./data" });
}

TEST_CASE("config - missing input field returns error", "[config]") {
    temp_toml cfg{
        "[main]\n"
        "output = '/data/output'\n"
    };

    fake_argv args{ {"app", "--config", cfg.str()} };
    config_parser parser;
    auto [config, err] = parser.parse(args.argc(), args.argv());

    CHECK(err);
}

TEST_CASE("config - nonexistent file returns error", "[config]") {
    fake_argv args{ {"app", "--config", "/nonexistent/path/config.toml"} };
    config_parser parser;
    auto [config, err] = parser.parse(args.argc(), args.argv());

    CHECK(err);
}

TEST_CASE("config - invalid toml syntax returns error", "[config]") {
    temp_toml cfg{
        "[main\n"
        "input = './data'\n"
    };

    fake_argv args{ {"app", "--config", cfg.str()} };
    config_parser parser;
    auto [config, err] = parser.parse(args.argc(), args.argv());

    CHECK(err);
}