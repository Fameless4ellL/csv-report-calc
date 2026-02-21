/**
 * \file test_csv_reader.cpp
 * \brief Unit-тесты для csv_reader
 *
 * Использует временные файлы через std::filesystem::temp_directory_path()
 * для изоляции тестов от файловой системы проекта.
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

#include "reader.hpp"

using csv_median::csv_reader;
using csv_median::csv_record;
using Catch::Approx;

namespace fs = std::filesystem;

/**
 * \brief RAII-обёртка для временной директории.
 * Удаляет директорию и всё содержимое при уничтожении.
 */
struct temp_dir {
    fs::path path;

    temp_dir() {
        path = fs::temp_directory_path()
            / ("csv_reader_test_" + std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()
            ));
        fs::create_directories(path);
    }

    ~temp_dir() {
        fs::remove_all(path);
    }

    // Создать файл с содержимым внутри временной директории
    fs::path make_file(const std::string& name_,
        const std::string& content_) const
    {
        const auto file_path = path / name_;
        std::ofstream f{ file_path };
        f << content_;
        return file_path;
    }
};

TEST_CASE("reader - read trade.csv", "[csv]") {
    temp_dir tmp;
    tmp.make_file("trade.csv",
        "receive_ts;exchange_ts;price;quantity;side\n"
        "1000;900;100.00000000;1.00000000;bid\n"
        "2000;1900;101.00000000;2.00000000;ask\n"
        "3000;2900;102.00000000;3.00000000;bid\n"
    );

    csv_reader reader;
    auto [records, err] = reader.load(tmp.path, {});

    REQUIRE_FALSE(err);
    REQUIRE(records.size() == 3);

    CHECK(records[0].receive_ts == 1000);
    CHECK(records[0].price == Approx(100.0));

    CHECK(records[1].receive_ts == 2000);
    CHECK(records[1].price == Approx(101.0));

    CHECK(records[2].receive_ts == 3000);
    CHECK(records[2].price == Approx(102.0));
}

TEST_CASE("reader - read level.csv", "[csv]") {
    temp_dir tmp;
    tmp.make_file("level.csv",
        "receive_ts;exchange_ts;price;quantity;side;rebuild\n"
        "1000;900;68480.00000000;10.00000000;bid;1\n"
        "1000;900;68479.90000000;0.50000000;bid;0\n"
    );

    csv_reader reader;
    auto [records, err] = reader.load(tmp.path, {});

    REQUIRE_FALSE(err);
    REQUIRE(records.size() == 2);
    CHECK(records[0].price == Approx(68480.0));
    CHECK(records[1].price == Approx(68479.9));
}

TEST_CASE("reader - sort receive_ts", "[csv]") {
    temp_dir tmp;
    tmp.make_file("trade.csv",
        "receive_ts;exchange_ts;price;quantity;side\n"
        "3000;2900;103.00000000;1.00000000;bid\n"
        "1000;900;101.00000000;1.00000000;bid\n"
        "2000;1900;102.00000000;1.00000000;ask\n"
    );

    csv_reader reader;
    auto [records, err] = reader.load(tmp.path, {});

    REQUIRE_FALSE(err);
    REQUIRE(records.size() == 3);

    CHECK(records[0].receive_ts == 1000);
    CHECK(records[1].receive_ts == 2000);
    CHECK(records[2].receive_ts == 3000);
}

TEST_CASE("reader - merge multiple files", "[csv]") {
    temp_dir tmp;
    tmp.make_file("trade.csv",
        "receive_ts;exchange_ts;price;quantity;side\n"
        "1000;900;100.00000000;1.00000000;bid\n"
        "3000;2900;300.00000000;1.00000000;bid\n"
    );
    tmp.make_file("level.csv",
        "receive_ts;exchange_ts;price;quantity;side;rebuild\n"
        "2000;1900;200.00000000;1.00000000;ask;1\n"
        "4000;3900;400.00000000;1.00000000;ask;0\n"
    );

    csv_reader reader;
    auto [records, err] = reader.load(tmp.path, {});

    REQUIRE_FALSE(err);
    REQUIRE(records.size() == 4);

    CHECK(records[0].receive_ts == 1000);
    CHECK(records[1].receive_ts == 2000);
    CHECK(records[2].receive_ts == 3000);
    CHECK(records[3].receive_ts == 4000);
}

TEST_CASE("csv_reader - filter by filename_mask", "[csv]") {
    temp_dir tmp;
    tmp.make_file("btcusdt_trade_2024.csv",
        "receive_ts;exchange_ts;price;quantity;side\n"
        "1000;900;100.00000000;1.00000000;bid\n"
    );
    tmp.make_file("btcusdt_level_2024.csv",
        "receive_ts;exchange_ts;price;quantity;side;rebuild\n"
        "2000;1900;200.00000000;1.00000000;ask;1\n"
    );
    tmp.make_file("other_data.csv",
        "receive_ts;exchange_ts;price;quantity;side\n"
        "3000;2900;300.00000000;1.00000000;bid\n"
    );

    csv_reader reader;

    SECTION("'trade'") {
        auto [records, err] = reader.load(tmp.path, { "trade" });
        REQUIRE_FALSE(err);
        REQUIRE(records.size() == 1);
        CHECK(records[0].receive_ts == 1000);
    }

    SECTION("'trade' & 'level'") {
        auto [records, err] = reader.load(tmp.path, { "trade", "level" });
        REQUIRE_FALSE(err);
        REQUIRE(records.size() == 2);
    }

    SECTION("empty") {
        auto [records, err] = reader.load(tmp.path, {});
        REQUIRE_FALSE(err);
        REQUIRE(records.size() == 3);
    }
}

TEST_CASE("reader - empty file", "[csv]") {
    temp_dir tmp;
    tmp.make_file("trade.csv", "");

    csv_reader reader;
    auto [records, err] = reader.load(tmp.path, {});

    CHECK_FALSE(err);
    CHECK(records.empty());
}

TEST_CASE("csv_reader - only header", "[csv]") {
    temp_dir tmp;
    tmp.make_file("trade.csv",
        "receive_ts;exchange_ts;price;quantity;side\n"
    );

    csv_reader reader;
    auto [records, err] = reader.load(tmp.path, {});

    CHECK_FALSE(err);
    CHECK(records.empty());
}

TEST_CASE("reader - wrong value rows are skipped", "[csv]") {
    temp_dir tmp;
    tmp.make_file("trade.csv",
        "receive_ts;exchange_ts;price;quantity;side\n"
        "1000;900;100.00000000;1.00000000;bid\n"
        "not_a_number;900;100.00000000;1.00000000;bid\n"
        "2000;1900;not_a_price;1.00000000;ask\n"
        "3000;2900;102.00000000;1.00000000;bid\n"
    );

    csv_reader reader;
    auto [records, err] = reader.load(tmp.path, {});

    CHECK_FALSE(err);
    REQUIRE(records.size() == 2);
    CHECK(records[0].receive_ts == 1000);
    CHECK(records[1].receive_ts == 3000);
}

TEST_CASE("reader - dir is not exist", "[csv]") {
    csv_reader reader;
    auto [records, err] = reader.load("/nonexistent/path/to/dir", {});

    CHECK(err);
    CHECK(records.empty());
}

TEST_CASE("reader - dir without .csv", "[csv]") {
    temp_dir tmp;
    tmp.make_file("readme.txt", "some text");

    csv_reader reader;
    auto [records, err] = reader.load(tmp.path, {});

    CHECK_FALSE(err);
    CHECK(records.empty());
}

TEST_CASE("reader - empty needed col", "[csv]") {
    temp_dir tmp;
    tmp.make_file("trade.csv",
        "timestamp;exchange_ts;cost;quantity;side\n"
        "1000;900;100.0;1.0;bid\n"
    );

    csv_reader reader;
    auto [records, err] = reader.load(tmp.path, {});

    CHECK(records.empty());
}