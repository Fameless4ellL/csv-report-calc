/**
 * \file reader.hpp
 * \brief Чтение CSV файлов биржевых данных
 */

#pragma once

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ranges>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include <spdlog/spdlog.h>

namespace csv_median {

    namespace fs = std::filesystem;

    /**
     * \brief Одна запись из CSV файла
     */
    struct csv_record {
        std::uint64_t receive_ts;
        double        price;
    };

    /**
     * \brief Читатель CSV файлов биржевых данных
     *
     * Поддерживает файлы форматов level.csv и trade.csv.
     * Разделитель колонок — точка с запятой (;).
     */
    class csv_reader {
    public:
        csv_reader() noexcept = default;

        /**
         * \brief Загрузить все подходящие CSV файлы из директории
         * \param input_dir_   директория с входными файлами
         * \param masks_       список масок имён файлов (пустой — все файлы)
         * \return отсортированный по receive_ts вектор записей и код ошибки
         */
        [[nodiscard]] std::tuple<std::vector<csv_record>, std::error_code>
            load(const fs::path& input_dir_,
                const std::vector<std::string>& masks_) noexcept;

    private:
        /**
         * \brief Найти подходящие CSV файлы в директории
         * \return список путей к файлам
         */
        [[nodiscard]] std::tuple<std::vector<fs::path>, std::error_code>
            scan_directory(const fs::path& dir_,
                const std::vector<std::string>& masks_) noexcept;

        /**
         * \brief Прочитать один CSV файл
         * \return вектор записей и код ошибки
         */
        [[nodiscard]] std::tuple<std::vector<csv_record>, std::error_code>
            read_file(const fs::path& path_) noexcept;

        /**
         * \brief Найти индекс колонки по имени в заголовке
         * \return индекс или -1 если не найдено
         */
        [[nodiscard]] int find_column(
            const std::string& header_,
            std::string_view   column_name_) const noexcept;

        /**
         * \brief Проверить, проходит ли файл по маскам
         */
        [[nodiscard]] bool matches_masks(
            const fs::path& path_,
            const std::vector<std::string>& masks_) const noexcept;

        /**
         * \brief Разбить строку по разделителю на части
         */
        [[nodiscard]] std::vector<std::string_view>
            split(std::string_view line_, char delim_) const noexcept;
    };

    // ──────────────────────────────────────────────
    // Реализация
    // ──────────────────────────────────────────────

    inline bool csv_reader::matches_masks(
        const fs::path& path_,
        const std::vector<std::string>& masks_) const noexcept
    {
        // Пустой список масок — читаем все файлы
        if (masks_.empty()) {
            return true;
        }
        const auto filename = path_.stem().string();
        return std::ranges::any_of(masks_, [&filename](const auto& mask) {
            return filename.find(mask) != std::string::npos;
            });
    }

    inline std::vector<std::string_view>
        csv_reader::split(std::string_view line_, char delim_) const noexcept {
        std::vector<std::string_view> parts;
        std::size_t start = 0;

        while (start < line_.size()) {
            const auto pos = line_.find(delim_, start);
            if (pos == std::string_view::npos) {
                parts.push_back(line_.substr(start));
                break;
            }
            parts.push_back(line_.substr(start, pos - start));
            start = pos + 1;
        }
        return parts;
    }

    inline int csv_reader::find_column(
        const std::string& header_,
        std::string_view   column_name_) const noexcept
    {
        const auto parts = split(header_, ';');
        for (int i = 0; i < static_cast<int>(parts.size()); ++i) {
            if (parts[static_cast<std::size_t>(i)] == column_name_) {
                return i;
            }
        }
        return -1;
    }

    inline std::tuple<std::vector<csv_record>, std::error_code>
        csv_reader::read_file(const fs::path& path_) noexcept {
        std::vector<csv_record> records;

        std::ifstream file{ path_ };
        if (!file.is_open()) {
            spdlog::error("can't open file: {}", path_.string());
            return { {}, std::make_error_code(std::errc::no_such_file_or_directory) };
        }

        std::string header_line;
        if (!std::getline(file, header_line)) {
            spdlog::warn("file is empty: {}", path_.string());
            return { {}, {} };
        }

        const int ts_col = find_column(header_line, "receive_ts");
        const int price_col = find_column(header_line, "price");

        if (ts_col < 0 || price_col < 0) {
            spdlog::error(
                "file {} is not containing required cols receive_ts/price",
                path_.string());
            return { {}, std::make_error_code(std::errc::invalid_argument) };
        }

        std::string line;
        std::size_t line_num = 1;

        while (std::getline(file, line)) {
            ++line_num;

            if (line.empty()) {
                continue;
            }

            const auto parts = split(line, ';');
            const auto ts_idx = static_cast<std::size_t>(ts_col);
            const auto price_idx = static_cast<std::size_t>(price_col);

            if (parts.size() <= std::max(ts_idx, price_idx)) {
                spdlog::warn("{}:{} — not enough cols, pass",
                    path_.string(), line_num);
                continue;
            }

            csv_record rec{};

            // Парсинг receive_ts
            const auto ts_sv = parts[ts_idx];
            const auto [ts_end, ts_err] = std::from_chars(
                ts_sv.data(), ts_sv.data() + ts_sv.size(), rec.receive_ts);

            if (ts_err != std::errc{}) {
                spdlog::warn("{}:{} — wrong format on receive_ts '{}', pass",
                    path_.string(), line_num, ts_sv);
                continue;
            }

            // Парсинг price (std::from_chars для double — C++17+)
            const auto price_sv = parts[price_idx];
            const auto [p_end, p_err] = std::from_chars(
                price_sv.data(), price_sv.data() + price_sv.size(), rec.price);

            if (p_err != std::errc{}) {
                spdlog::warn("{}:{} — wrong price '{}', pass",
                    path_.string(), line_num, price_sv);
                continue;
            }

            records.push_back(rec);
        }

        spdlog::info("  {} — done {} records size", path_.filename().string(),
            records.size());
        return { std::move(records), {} };
    }

    inline std::tuple<std::vector<fs::path>, std::error_code>
        csv_reader::scan_directory(
            const fs::path& dir_,
            const std::vector<std::string>& masks_) noexcept
    {
        if (!fs::exists(dir_)) {
            spdlog::error("input dir doesn't exist: {}", dir_.string());
            return { {}, std::make_error_code(std::errc::no_such_file_or_directory) };
        }
        if (!fs::is_directory(dir_)) {
            spdlog::error("choosen path is not directory: {}",
                dir_.string());
            return { {}, std::make_error_code(std::errc::not_a_directory) };
        }

        std::vector<fs::path> paths;

        try {
            for (const auto& entry : fs::directory_iterator{ dir_ }) {
                if (!entry.is_regular_file()) {
                    continue;
                }
                if (entry.path().extension() != ".csv") {
                    continue;
                }
                if (matches_masks(entry.path(), masks_)) {
                    paths.push_back(entry.path());
                }
            }
        }
        catch (const fs::filesystem_error& e) {
            spdlog::error("error scannig dir {}: {}",
                dir_.string(), e.what());
            return { {}, e.code() };
        }

        std::ranges::sort(paths);
        return { std::move(paths), {} };
    }

    inline std::tuple<std::vector<csv_record>, std::error_code>
        csv_reader::load(
            const fs::path& input_dir_,
            const std::vector<std::string>& masks_) noexcept
    {
        auto [paths, scan_err] = scan_directory(input_dir_, masks_);
        if (scan_err) {
            return { {}, scan_err };
        }

        if (paths.empty()) {
            spdlog::warn("can't see ant .csv file at: {}",
                input_dir_.string());
            return { {}, {} };
        }

        spdlog::info("Not Found: {}", paths.size());
        for (const auto& p : paths) {
            spdlog::info("  - {}", p.filename().string());
        }

        std::vector<csv_record> all_records;

        for (const auto& path : paths) {
            auto [records, read_err] = read_file(path);
            if (read_err) {
                // Продолжаем обработку остальных файлов
                spdlog::warn("passing file {} due error",
                    path.filename().string());
                continue;
            }
            all_records.insert(
                all_records.end(),
                std::make_move_iterator(records.begin()),
                std::make_move_iterator(records.end())
            );
        }

        // Сортировка по временной метке — ключевой шаг перед расчётом медианы
        std::ranges::sort(all_records, {}, &csv_record::receive_ts);

        spdlog::info("records size: {}", all_records.size());
        return { std::move(all_records), {} };
    }

} // namespace csv_median