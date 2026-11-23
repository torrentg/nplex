#include "filesystem_snapshot_persister.hpp"
#include "snapshot_base.hpp"
#include "exception.hpp"
#include "logger.hpp"
#include <filesystem>
#include <map>

namespace fs = std::filesystem;

// ==========================================================
// Internal (static) functions
// ==========================================================

namespace {

constexpr std::string FILENAME_PREFIX = "snapshot-";
constexpr std::string FILENAME_EXTENSION = ".dat";

std::map<nplex::rev_t, std::string> get_catalog(const fs::path &directory)
{
    std::map<nplex::rev_t, std::string> catalog;

    if (!fs::exists(directory))
        return catalog;

    for (const auto &entry : fs::directory_iterator(directory))
    {
        if (!entry.is_regular_file())
            continue;

        const std::string file_name = entry.path().filename().string();

        if (!file_name.starts_with(FILENAME_PREFIX))
            continue;

        if (!file_name.ends_with(FILENAME_EXTENSION))
            continue;

        try
        {
            auto header = nplex::read_snapshot_file_header(entry.path());

            auto it = catalog.find(header.rev);

            if (it != catalog.end())
                throw nplex::nplex_exception("Found duplicated snapshots: rev={}, file1={}, file2={}", header.rev, it->second, file_name);

            catalog[header.rev] = file_name;
        }
        catch (const std::exception &e)
        {
            LOG_WARN("Unable to read {}: {}", file_name, e.what());
            continue;
        }
    }

    return catalog;
}

} // unnamed namespace

// ==========================================================
// filesystem_snapshot_persister_t methods
// ==========================================================

nplex::filesystem_snapshot_persister_t::filesystem_snapshot_persister_t(const fs::path &path) : m_path(path)
{
    std::error_code ec;

    const auto status = fs::status(m_path, ec);

    if (ec || !fs::exists(status) || !fs::is_directory(status))
        throw nplex_exception("snapshots directory does not exist or is not a directory: {}", m_path.string());

    const auto perms = fs::status(m_path, ec).permissions();

    if (ec)
        throw nplex_exception("unable to access snapshots directory: {}", m_path.string());

    if ((perms & fs::perms::owner_read) == fs::perms::none &&
        (perms & fs::perms::group_read) == fs::perms::none &&
        (perms & fs::perms::others_read) == fs::perms::none)
    {
        throw nplex_exception("snapshots directory is not readable: {}", m_path.string());
    }
}

std::pair<nplex::rev_t, nplex::rev_t> nplex::filesystem_snapshot_persister_t::get_snapshots_range()
{
    std::pair<nplex::rev_t, nplex::rev_t> range{0, 0};

    auto catalog = ::get_catalog(m_path);

    while (!catalog.empty())
    {
        auto it = catalog.begin();

        try {
            auto [rev, timestamp, snapshot] = read_snapshot_file(m_path / it->second);
            range = {rev, rev};
            break;
        }
        catch (const std::exception &e) {
            LOG_WARN("Failed to read file {}: {}", it->second, e.what());
            catalog.erase(it);
        }
    }

    while (catalog.size() > 1)
    {
        auto it = std::prev(catalog.end());

        try {
            auto [rev, timestamp, snapshot] = read_snapshot_file(m_path / it->second);
            range.second = rev;
            break;
        }
        catch (const std::exception &e) {
            LOG_WARN("Failed to read file {}: {}", it->second, e.what());
            catalog.erase(it);
        }
    }

    if (spdlog::should_log(spdlog::level::debug))
        for (const auto &[rev, file] : catalog)
            LOG_DEBUG("{} added to catalog, rev={}", file, rev);

    LOG_DEBUG("Snapshot catalog complete: range=[{}-{}], entries={}", 
        (catalog.empty() ? 0 : catalog.begin()->first), 
        (catalog.empty() ? 0 : catalog.rbegin()->first), 
        catalog.size());

    return range;
}

std::tuple<nplex::rev_t, nplex::millis_t, std::string> nplex::filesystem_snapshot_persister_t::read_snapshot(rev_t rev)
{
    auto catalog = get_catalog(m_path);

    while (!catalog.empty())
    {
        auto it = catalog.upper_bound(rev);

        if (it == catalog.begin())
            break;

        it = std::prev(it);

        try {
            return read_snapshot_file(m_path / it->second);
        }
        catch (const std::exception &e) {
            LOG_WARN("Failed to read file {}: {}", it->second, e.what());
            catalog.erase(it);
        }
    }

    return {0, millis_t{0}, ""};
}

void nplex::filesystem_snapshot_persister_t::write_snapshot(rev_t rev, millis_t timestamp, std::string_view data)
{
    if (rev == 0 || data.empty())
        throw nplex_exception("snapshot payload is empty");

    auto filename = FILENAME_PREFIX + std::to_string(rev) + FILENAME_EXTENSION;

    try {
        write_snapshot_file(rev, timestamp, m_path / filename, data);
    }
    catch (...) {
        LOG_ERROR("Failed to write file {}", filename);
        throw;
    }
}
