#include "snapshot_catalog.hpp"
#include "exception.hpp"
#include "logger.hpp"

namespace fs = std::filesystem;

// ==========================================================
// Internal (static) functions
// ==========================================================

namespace {

bool is_file_unchanged(const fs::path &path, const nplex::snapshot_file_t &file)
{
    auto file_size = fs::file_size(path);
    auto last_write = fs::last_write_time(path);

    return (file_size == file.file_size && last_write == file.last_write);
}

bool contains(const std::pair<nplex::rev_t, nplex::rev_t> &range, nplex::rev_t rev)
{
    if (range.first == 0 || range.second < range.first)
        return true;

    return (range.first <= rev && rev <= range.second);
}

} // unnamed namespace

// ==========================================================
// snapshot_catalog_builder_t methods
// ==========================================================

nplex::snapshot_catalog_builder_t & nplex::snapshot_catalog_builder_t::with_path(const std::filesystem::path &path){
    m_path = path;
    return *this;
}

nplex::snapshot_catalog_builder_t & nplex::snapshot_catalog_builder_t::with_files_prefix(const std::string &prefix){
    m_files_prefix = prefix;
    return *this;
}

nplex::snapshot_catalog_builder_t & nplex::snapshot_catalog_builder_t::with_files_extension(const std::string &ext){
    m_files_extension = ext;
    return *this;
}

nplex::snapshot_catalog_builder_t & nplex::snapshot_catalog_builder_t::with_range(rev_t first_rev, rev_t last_rev){
    assert(first_rev <= last_rev);
    m_range = {first_rev, last_rev};
    return *this;
}

nplex::snapshot_catalog_builder_t & nplex::snapshot_catalog_builder_t::with_range(std::pair<rev_t, rev_t> range){
    assert(range.first <= range.second);
    m_range = range;
    return *this;
}

nplex::snapshot_catalog_t nplex::snapshot_catalog_builder_t::build(const snapshot_catalog_t &prev_catalog)
{
    snapshot_catalog_t catalog{};

    if (m_path.empty() || m_files_prefix.empty() || m_files_extension.empty())
        throw nplex_exception("invalid configuration");

    std::map<std::string, const snapshot_file_t &> prev_files;

    for (const auto &[rev, segment] : prev_catalog)
        prev_files.emplace(segment.file_name, segment);

    // Scan the directory for new snapshot files
    for (const auto &entry : fs::directory_iterator(m_path))
    {
        if (!entry.is_regular_file())
            continue;

        const std::string file_name = entry.path().filename().string();

        if (!file_name.starts_with(m_files_prefix))
            continue;

        if (!file_name.ends_with(m_files_extension))
            continue;

        // Check if the file was present in the previous catalog
        auto it1 = prev_files.find(file_name);

        if (it1 != prev_files.end() && ::is_file_unchanged(entry.path(), it1->second))
        {
            if (!::contains(m_range, it1->second.rev))
                continue;

            // Create a snapshot file entry from previous catalog
            catalog.emplace(
                it1->second.rev, 
                it1->second
            );

            continue;
        }

        try
        {
            auto file = snapshot_file_t{entry.path()};

            if (!::contains(m_range, file.rev))
                continue;

            auto it2 = catalog.find(file.rev);

            if (it2 != catalog.end()) {
                LOG_WARN("Found duplicated snapshot for revision {}: {} and {}", file.rev, it2->second.file_name, file_name);
                continue;
            }

            catalog[file.rev] = file;
        }
        catch (const std::exception &e)
        {
            LOG_WARN("unable to read {}: {}", file_name, e.what());
            continue;
        }
    }

    if (spdlog::should_log(spdlog::level::trace))
        for (const auto &[rev, segment] : catalog)
            LOG_TRACE("Snapshot catalog entry {}: {}", rev, segment.file_name);

    return catalog;
}
