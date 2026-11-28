#include "filesystem_snapshot_persister.hpp"
#include "snapshot_catalog.hpp"
#include "exception.hpp"
#include "logger.hpp"
#include "utils.hpp"

namespace fs = std::filesystem;

// ==========================================================
// Internal (static) functions
// ==========================================================

namespace {

constexpr std::string FILENAME_PREFIX = "snapshot-";
constexpr std::string FILENAME_EXTENSION = ".dat";
constexpr nplex::millis_t MIN_CATALOG_UPDATE_INTERVAL = std::chrono::minutes(5);

} // unnamed namespace

// ==========================================================
// filesystem_snapshot_persister_t methods
// ==========================================================

nplex::filesystem_snapshot_persister_t::~filesystem_snapshot_persister_t() {
    // Defined out-of-line because snapshot_file_t is forward-declared
    // in the header and must be a complete type at the destructor definition.
}

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

    update_catalog();
}

std::pair<nplex::rev_t, nplex::rev_t> nplex::filesystem_snapshot_persister_t::get_snapshots_range()
{
    std::lock_guard lock(m_catalog_mutex);

    if (m_catalog.empty())
        return {0, 0};

    return {m_catalog.begin()->first, m_catalog.rbegin()->first};
}

std::tuple<nplex::rev_t, nplex::millis_t, std::string> nplex::filesystem_snapshot_persister_t::read_snapshot(rev_t rev)
{
    auto [srev, sts, sdata] = read_snapshot_from_catalog(rev);

    if (srev != 0)
        return {srev, sts, sdata};

    if (update_catalog())
        return read_snapshot_from_catalog(rev);

    return {0, millis_t{0}, ""};
}

std::tuple<nplex::rev_t, nplex::millis_t, std::string> nplex::filesystem_snapshot_persister_t::read_snapshot_from_catalog(rev_t rev)
{
    while (!m_catalog.empty())
    {
        auto it = m_catalog.upper_bound(rev);

        if (it == m_catalog.begin())
            break;

        it = std::prev(it);

        try {
            return read_snapshot_file(m_path / it->second.file_name);
        }
        catch (const std::exception &e) {
            LOG_WARN("Failed to read file {}: {}", it->second.file_name, e.what());
            m_catalog.erase(it);
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

bool nplex::filesystem_snapshot_persister_t::update_catalog(bool force)
{
    std::lock_guard lock(m_catalog_mutex);
    auto now = nplex::now();

    // Skip catalog update if it was recently updated
    if (!force && m_last_catalog_update != millis_t::zero() && now < m_last_catalog_update + MIN_CATALOG_UPDATE_INTERVAL)
        return false;

    snapshot_catalog_builder_t builder;

    builder.with_path(m_path)
           .with_files_prefix(FILENAME_PREFIX)
           .with_files_extension(FILENAME_EXTENSION);

    m_catalog = builder.build(m_catalog);
    m_last_catalog_update = now;

    [[maybe_unused]] rev_t min_rev = (m_catalog.empty() ? 0 : m_catalog.begin()->first);
    [[maybe_unused]] rev_t max_rev = (m_catalog.empty() ? 0 : m_catalog.rbegin()->first);

    LOG_DEBUG("Snapshot catalog update complete: range {}-{}, entries={}", min_rev, max_rev, m_catalog.size());

    return true;
}
