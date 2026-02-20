/**
 * \file writer.hpp
 * \brief Запись результатов расчёта медианы в CSV файл
 */

#pragma once

#include <cstdint>
#include <filesystem>
#include <format>
#include <fstream>
#include <tuple>

#include <spdlog/spdlog.h>

namespace csv_median {

    namespace fs = std::filesystem;

    /**
     * \brief Писатель результатов медианы в CSV формат
     */
    class result_writer {
    public:
        result_writer() noexcept = default;

        /**
         * \brief Открыть выходной файл для записи
         * \param output_dir_  директория для сохранения
         * \param filename_    имя выходного файла
         * \return код ошибки (пустой — успех)
         */
        [[nodiscard]] std::error_code
            open(const fs::path& output_dir_,
                const std::string& filename_ = "median_result.csv") noexcept;

        /**
         * \brief Записать строку результата
         * \param receive_ts_    временная метка события
         * \param price_median_  значение медианы
         * \return код ошибки
         */
        [[nodiscard]] std::error_code
            write(std::uint64_t receive_ts_, double price_median_) noexcept;

        /**
         * \brief Количество записанных строк
         */
        [[nodiscard]] std::size_t written_count() const noexcept;

        /**
         * \brief Закрыть файл и сбросить буферы
         */
        void close() noexcept;

        ~result_writer() noexcept;

    private:
        std::ofstream _file;
        std::size_t   _written_count{ 0 };
        fs::path      _output_path;

        static constexpr std::string_view k_header = "receive_ts;price_median\n";
    };

    // ──────────────────────────────────────────────
    // Реализация
    // ──────────────────────────────────────────────

    inline std::error_code result_writer::open(
        const fs::path& output_dir_,
        const std::string& filename_) noexcept
    {
        try {
            if (!fs::exists(output_dir_)) {
                fs::create_directories(output_dir_);
                spdlog::info("created input dir: {}",
                    output_dir_.string());
            }

            _output_path = output_dir_ / filename_;
            _file.open(_output_path, std::ios::out | std::ios::trunc);

            if (!_file.is_open()) {
                spdlog::error("can't created output file: {}",
                    _output_path.string());
                return std::make_error_code(std::errc::permission_denied);
            }

            _file << k_header;
            return {};

        }
        catch (const fs::filesystem_error& e) {
            spdlog::error("can't create dir {}: {}",
                output_dir_.string(), e.what());
            return e.code();
        }
    }

    inline std::error_code result_writer::write(
        std::uint64_t receive_ts_,
        double        price_median_) noexcept
    {
        // Форматируем с точностью 8 знаков, как в исходных данных
        _file << std::format("{};{:.8f}\n", receive_ts_, price_median_);

        if (_file.fail()) {
            spdlog::error("error during reading file: {}", _output_path.string());
            return std::make_error_code(std::errc::io_error);
        }

        ++_written_count;
        return {};
    }

    inline std::size_t result_writer::written_count() const noexcept {
        return _written_count;
    }

    inline void result_writer::close() noexcept {
        if (_file.is_open()) {
            _file.flush();
            _file.close();
        }
    }

    inline result_writer::~result_writer() noexcept {
        close();
    }

}