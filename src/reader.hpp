/**
 * \file csv_reader.hpp
 * \brief Потоковое чтение CSV файлов биржевых данных
 *
 * Архитектура: каждый файл читается построчно через file_cursor.
 * Записи не накапливаются в памяти — каждая передаётся в callback
 * сразу после парсинга. Это позволяет обрабатывать файлы любого размера.
 *
 * K-way merge реализован через min-heap курсоров по открытым файлам,
 * что даёт O(N log k) без хранения всех данных одновременно.
 * Память: O(k) где k — число файлов, не O(N) от числа записей.
 */

#pragma once

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <optional>
#include <queue>
#include <ranges>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <vector>

#include <spdlog/spdlog.h>

#include "pool.hpp"

namespace csv_median {

    namespace fs = std::filesystem;

    // Размер буфера чтения файла — 64 KB компромисс между памятью и I/O
    inline constexpr std::size_t k_read_buffer_size = 64 * 1024;

    /**
     * \brief Одна запись из CSV файла
     */
    struct csv_record {
        std::uint64_t receive_ts;
        double        price;
    };

    /**
     * \brief Курсор для построчного чтения одного CSV файла
     *
     * Держит открытый файл и читает записи по одной без загрузки в память.
     * Используется в k-way merge — одновременно в памяти только k записей.
     */
    class file_cursor {
    public:
        explicit file_cursor(const fs::path& path_) noexcept;

        /**
         * \brief Продвинуть курсор к следующей записи
         * \return true если есть следующая запись
         */
        [[nodiscard]] bool advance() noexcept;

        /**
         * \brief Курсор валиден и готов к чтению
         */
        [[nodiscard]] bool is_valid() const noexcept;

        /**
         * \brief Текущая запись курсора
         */
        [[nodiscard]] const csv_record& current() const noexcept;

        /**
         * \brief Имя файла для логирования
         */
        [[nodiscard]] std::string filename() const noexcept;

    private:
        [[nodiscard]] int find_column(
            std::string_view header_,
            std::string_view col_) const noexcept;

        [[nodiscard]] bool parse_line(std::string_view line_) noexcept;

        fs::path      _path;
        std::ifstream _file;
        std::string   _line;
        csv_record    _current{};
        int           _ts_col{ -1 };
        int           _price_col{ -1 };
        std::size_t   _line_num{ 0 };
        bool          _valid{ false };
    };

    /**
     * \brief Потоковый читатель CSV файлов
     *
     * Принимает внешний pool
     */
    class csv_reader {
    public:
        /**
         * \brief Создает читатель с внешним thread pool
         * \param pool_ пул потоков для параллельного открытия файлов
         */
        explicit csv_reader(thread_pool& pool_) noexcept;

        /**
         * \brief Записи в потоковом режиме
         * \param input_dir_  директория с входными файлами
         * \param masks_      маски имён файлов (пустой список — все файлы)
         * \param on_record_  callback для каждой записи
         * \return код ошибки
         */
        [[nodiscard]] std::error_code
            process(const fs::path& input_dir_,
                const std::vector<std::string>& masks_,
                std::function<void(const csv_record&)> on_record_) noexcept;

    private:
        [[nodiscard]] std::tuple<std::vector<fs::path>, std::error_code>
            scan_directory(const fs::path& dir_,
                const std::vector<std::string>& masks_) noexcept;

        [[nodiscard]] bool matches_masks(
            const fs::path& path_,
            const std::vector<std::string>& masks_) const noexcept;

        thread_pool& _pool;
    };


    inline file_cursor::file_cursor(const fs::path& path_) noexcept
        : _path{ path_ }
    {
        // меньше системных вызовов
        _file.rdbuf()->pubsetbuf(nullptr, static_cast<std::streamsize>(
            k_read_buffer_size));
        _file.open(path_);

        if (!_file.is_open()) [[unlikely]] {
            spdlog::error("Failed to open file: {}", path_.string());
            return;
        }

        std::string header;
        if (!std::getline(_file, header)) [[unlikely]] {
            spdlog::warn("File is empty: {}", path_.string());
            return;
        }

        _ts_col = find_column(header, "receive_ts");
        _price_col = find_column(header, "price");

        if (_ts_col < 0 || _price_col < 0) [[unlikely]] {
            spdlog::error("File {} missing required columns receive_ts/price",
                path_.string());
            return;
        }
        _valid = advance();
    }

    inline bool file_cursor::is_valid() const noexcept {
        return _valid;
    }

    inline const csv_record& file_cursor::current() const noexcept {
        return _current;
    }

    inline std::string file_cursor::filename() const noexcept {
        return _path.filename().string();
    }

    inline int file_cursor::find_column(
        std::string_view header_,
        std::string_view col_) const noexcept
    {
        std::size_t start = 0;
        int idx = 0;

        while (start <= header_.size()) {
            const auto pos = header_.find(';', start);
            const auto end = (pos == std::string_view::npos)
                ? header_.size() : pos;

            if (header_.substr(start, end - start) == col_) {
                return idx;
            }
            ++idx;
            start = (pos == std::string_view::npos) ? header_.size() + 1 : pos + 1;
        }
        return -1;
    }

    inline bool file_cursor::parse_line(std::string_view line_) noexcept {
        const auto ts_idx = static_cast<std::size_t>(_ts_col);
        const auto price_idx = static_cast<std::size_t>(_price_col);
        const auto max_idx = std::max(ts_idx, price_idx);

        std::size_t start = 0;
        std::size_t col = 0;
        std::string_view ts_sv, price_sv;

        while (col <= max_idx) {
            const auto pos = line_.find(';', start);
            const auto end = (pos == std::string_view::npos)
                ? line_.size() : pos;

            if (col == ts_idx) { ts_sv = line_.substr(start, end - start); }
            if (col == price_idx) { price_sv = line_.substr(start, end - start); }

            if (pos == std::string_view::npos) { break; }
            ++col;
            start = pos + 1;
        }

        if (ts_sv.empty() || price_sv.empty()) [[unlikely]] {
            return false;
        }

        const auto [ts_end, ts_err] = std::from_chars(
            ts_sv.data(), ts_sv.data() + ts_sv.size(), _current.receive_ts);

        if (ts_err != std::errc{}) [[unlikely]] {
            spdlog::warn("{}:{} - invalid receive_ts, skipping",
                _path.filename().string(), _line_num);
            return false;
        }

        const auto [p_end, p_err] = std::from_chars(
            price_sv.data(), price_sv.data() + price_sv.size(), _current.price);

        if (p_err != std::errc{}) [[unlikely]] {
            spdlog::warn("{}:{} - invalid price, skipping",
                _path.filename().string(), _line_num);
            return false;
        }

        return true;
    }

    inline bool file_cursor::advance() noexcept {
        while (std::getline(_file, _line)) {
            ++_line_num;

            if (_line.empty()) [[unlikely]] {
                continue;
            }

            if (parse_line(_line)) {
                return true;
            }
        }
        _valid = false;
        return false;
    }

    inline csv_reader::csv_reader(thread_pool& pool_) noexcept
        : _pool{ pool_ }
    {
    }

    inline bool csv_reader::matches_masks(
        const fs::path& path_,
        const std::vector<std::string>& masks_) const noexcept
    {
        if (masks_.empty()) {
            return true;
        }
        const auto stem = path_.stem().string();
        return std::ranges::any_of(masks_, [&stem](const auto& mask) {
            return stem.find(mask) != std::string::npos;
            });
    }

    inline std::tuple<std::vector<fs::path>, std::error_code>
        csv_reader::scan_directory(
            const fs::path& dir_,
            const std::vector<std::string>& masks_) noexcept
    {
        if (!fs::exists(dir_)) {
            spdlog::error("Input directory does not exist: {}", dir_.string());
            return { {}, std::make_error_code(std::errc::no_such_file_or_directory) };
        }
        if (!fs::is_directory(dir_)) {
            spdlog::error("Path is not a directory: {}", dir_.string());
            return { {}, std::make_error_code(std::errc::not_a_directory) };
        }

        std::vector<fs::path> paths;
        try {
            for (const auto& entry : fs::directory_iterator{ dir_ }) {
                if (!entry.is_regular_file()) { continue; }
                if (entry.path().extension() != ".csv") { continue; }
                if (matches_masks(entry.path(), masks_)) {
                    paths.push_back(entry.path());
                }
            }
        }
        catch (const fs::filesystem_error& e) {
            spdlog::error("Directory scan error {}: {}", dir_.string(), e.what());
            return { {}, e.code() };
        }

        std::ranges::sort(paths);
        return { std::move(paths), {} };
    }

    inline std::error_code
        csv_reader::process(
            const fs::path& input_dir_,
            const std::vector<std::string>& masks_,
            std::function<void(const csv_record&)> on_record_) noexcept
    {
        auto [paths, scan_err] = scan_directory(input_dir_, masks_);
        if (scan_err) {
            return scan_err;
        }

        if (paths.empty()) {
            spdlog::warn("No CSV files found in: {}", input_dir_.string());
            return {};
        }

        spdlog::info("Files found: {}", paths.size());

        using cursor_ptr = std::shared_ptr<file_cursor>;
        std::vector<std::future<cursor_ptr>> futures;
        futures.reserve(paths.size());

        for (const auto& path : paths) {
            futures.push_back(
                _pool.submit([path]() -> cursor_ptr {
                    return std::make_shared<file_cursor>(path);
                    })
            );
        }

        std::vector<cursor_ptr> cursors;
        cursors.reserve(paths.size());

        for (auto& f : futures) {
            auto cursor = f.get();
            if (cursor->is_valid()) {
                spdlog::info("  - {}", cursor->filename());
                cursors.push_back(std::move(cursor));
            }
        }

        if (cursors.empty()) {
            spdlog::warn("No data to process");
            return {};
        }

        using heap_entry = std::pair<std::uint64_t, std::size_t>; // (ts, idx)

        std::priority_queue<
            heap_entry,
            std::vector<heap_entry>,
            std::greater<heap_entry>
        > heap;

        for (std::size_t i = 0; i < cursors.size(); ++i) {
            heap.emplace(cursors[i]->current().receive_ts, i);
        }

        std::size_t total = 0;

        while (!heap.empty()) {
            const auto [ts, idx] = heap.top();
            heap.pop();

            on_record_(cursors[idx]->current());
            ++total;

            if (cursors[idx]->advance()) {
                heap.emplace(cursors[idx]->current().receive_ts, idx);
            }
        }

        spdlog::info("Records processed: {}", total);
        return {};
    }

}