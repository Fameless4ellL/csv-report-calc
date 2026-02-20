/**
 * \file config_parser.hpp
 * \brief Парсинг аргументов командной строки и TOML конфигурации
 */

#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include <boost/program_options.hpp>
#include <spdlog/spdlog.h>
#include <toml++/toml.hpp>

namespace csv_median {

    namespace fs = std::filesystem;
    namespace po = boost::program_options;

    /**
     * \brief Конфигурация приложения
     */
    struct app_config {
        fs::path              input_dir;
        fs::path              output_dir;
        std::vector<std::string> filename_masks;
    };

    /**
     * \brief Парсер конфигурации приложения
     *
     * Отвечает за чтение аргументов CLI и TOML файла.
     * Все ошибки возвращаются через std::error_code, без исключений.
     */
    class config_parser {
    public:
        config_parser() noexcept = default;

        /**
         * \brief Разобрать аргументы командной строки и загрузить конфиг
         * \param argc_ количество аргументов
         * \param argv_ массив аргументов
         * \return конфиг и код ошибки (пустой — успех)
         */
        [[nodiscard]] std::tuple<app_config, std::error_code>
            parse(int argc_, const char* const* argv_) noexcept;

    private:
        /**
         * \brief Найти путь к конфиг-файлу из аргументов CLI
         * \return путь к файлу или ошибку
         */
        [[nodiscard]] std::tuple<fs::path, std::error_code>
            resolve_config_path(int argc_, const char* const* argv_) noexcept;

        /**
         * \brief Загрузить и валидировать TOML файл
         * \param config_path_ путь к файлу конфигурации
         * \return конфиг и код ошибки
         */
        [[nodiscard]] std::tuple<app_config, std::error_code>
            load_toml(const fs::path& config_path_) noexcept;
    };

    // ──────────────────────────────────────────────
    // Реализация
    // ──────────────────────────────────────────────

    inline std::tuple<fs::path, std::error_code>
        config_parser::resolve_config_path(
            int argc_,
            const char* const* argv_) noexcept
    {
        try {
            po::options_description desc{ "csv_report_calc options" };
            desc.add_options()
                ("config", po::value<std::string>(), "path to config file")
                ("cfg",    po::value<std::string>(), "path to config file");

            po::variables_map vm;
            po::store(
                po::parse_command_line(argc_, argv_, desc),
                vm
            );
            po::notify(vm);

            if (vm.count("config")) {
                return { fs::path{vm["config"].as<std::string>()}, {} };
            }
            if (vm.count("cfg")) {
                return { fs::path{vm["cfg"].as<std::string>()}, {} };
            }

            // По умолчанию ищем config.toml рядом с исполняемым файлом
            const fs::path default_config =
                fs::path{ argv_[0] }.parent_path() / "config.toml";

            spdlog::info("argument -config is not used: {}",
                default_config.string());

            return { default_config, {} };

        }
        catch (const std::exception& e) {
            spdlog::error("error pasing args: {}", e.what());
            return { {}, std::make_error_code(std::errc::invalid_argument) };
        }
    }

    inline std::tuple<app_config, std::error_code>
        config_parser::load_toml(const fs::path& config_path_) noexcept {
        app_config config;

        if (!fs::exists(config_path_)) {
            spdlog::error("cfg file not found: {}", config_path_.string());
            return { {}, std::make_error_code(std::errc::no_such_file_or_directory) };
        }

        try {
            const auto table = toml::parse_file(config_path_.string());
            const auto main = table["main"];

            // input — обязательный параметр
            const auto input = main["input"].value<std::string>();
            if (!input) {
                spdlog::error("requred param is empty [main].input");
                return { {}, std::make_error_code(std::errc::invalid_argument) };
            }
            config.input_dir = fs::path{ *input };

            // output — опциональный, дефолт: ./output
            const auto output = main["output"].value<std::string>();
            config.output_dir = output
                ? fs::path{ *output }
            : fs::current_path() / "output";

            // filename_mask — опциональный список
            if (const auto masks = main["filename_mask"].as_array()) {
                for (const auto& mask : *masks) {
                    if (const auto str = mask.value<std::string>()) {
                        config.filename_masks.push_back(*str);
                    }
                }
            }

            return { config, {} };

        }
        catch (const toml::parse_error& e) {
            spdlog::error("error parsing TOML {}: {}", config_path_.string(),
                e.description());
            return { {}, std::make_error_code(std::errc::invalid_argument) };
        }
        catch (const std::exception& e) {
            spdlog::error("unexpected error during reading cfg: {}", e.what());
            return { {}, std::make_error_code(std::errc::io_error) };
        }
    }

    inline std::tuple<app_config, std::error_code>
        config_parser::parse(int argc_, const char* const* argv_) noexcept {
        auto [config_path, path_err] = resolve_config_path(argc_, argv_);
        if (path_err) {
            return { {}, path_err };
        }

        spdlog::info("reading file: {}", config_path.string());
        return load_toml(config_path);
    }

}