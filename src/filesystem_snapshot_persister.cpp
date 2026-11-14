#include "filesystem_snapshot_persister.hpp"
#include "buildinfo.hpp"
#include "exception.hpp"
#include "schema.hpp"
#include "logger.hpp"
#include "utils.hpp"
#include <filesystem>
#include <fstream>
#include <string_view>
#include <map>

namespace fs = std::filesystem;

// ==========================================================
// Internal (static) functions
// ==========================================================

namespace {

constexpr std::string FILENAME_PREFIX = "snapshot-";
constexpr std::string FILENAME_EXTENSION = ".dat";
constexpr std::string MAGIC_BYTES = "NPLEXSNP";

struct snapshot_file_header_t
{
    std::uint8_t magic[8];
    std::uint32_t schema2_hash;
    std::uint32_t checksum;
    std::uint64_t rev;

    explicit snapshot_file_header_t(std::uint64_t rev_ = 0, std::uint32_t chk = 0) : schema2_hash(SCHEMA2_HASH), checksum(chk), rev(rev_) {
        std::memcpy(magic, MAGIC_BYTES.data(), sizeof(magic));
    }
};

bool is_valid_header(const snapshot_file_header_t &header)
{
    if (std::memcmp(header.magic, MAGIC_BYTES.data(), sizeof(header.magic)) != 0)
        return false;

    if (header.schema2_hash != SCHEMA2_HASH)
        return false;

    if (header.rev == 0)
        return false;

    return true;
}

snapshot_file_header_t get_file_header(const fs::path &path)
{
    snapshot_file_header_t header;
    std::ifstream file(path, std::ios::binary);

    if (!file)
        throw nplex::nplex_exception("unable to open file");

    file.read(reinterpret_cast<char *>(&header), sizeof(header));

    if (!file)
        throw nplex::nplex_exception("unable to read snapshot header");

    if (!is_valid_header(header))
        throw nplex::nplex_exception("invalid snapshot header");

    return header;
}

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
            auto header = get_file_header(entry.path());

            auto it = catalog.find(header.rev);

            if (it != catalog.end())
                throw nplex::nplex_exception("Found duplicated snapshots: rev={}, file1={}, file2={}", header.rev, it->second, file_name);

            catalog[header.rev] = file_name;
        }
        catch (const std::exception &e)
        {
            LOG_WARN("{}", e.what());
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
            auto [rev, snapshot] = read_snapshot_file(m_path / it->second);
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
            auto [rev, snapshot] = read_snapshot_file(m_path / it->second);
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

std::pair<nplex::rev_t, std::string> nplex::filesystem_snapshot_persister_t::read_snapshot_file(const fs::path &path)
{
    std::streampos file_size = 0;
    snapshot_file_header_t header{};
    std::ifstream ifs(path, std::ios::binary);

    LOG_TRACE("Reading file {}", path.string());

    if (!ifs)
        throw nplex::nplex_exception("failed to open file");

    ifs.seekg(0, std::ios::end);
    file_size = ifs.tellg();

    ifs.seekg(0, std::ios::beg);
    ifs.read(reinterpret_cast<char *>(&header), sizeof(header));

    if (!ifs)
        throw nplex::nplex_exception("invalid snapshot file");

    if (!is_valid_header(header))
        throw nplex::nplex_exception("invalid snapshot header");

    auto snapshot_size = static_cast<std::size_t>(file_size) - sizeof(header);
    std::string snapshot(snapshot_size, '\0');

    ifs.read(snapshot.data(), static_cast<std::streamsize>(snapshot_size));

    if (!ifs)
        throw nplex::nplex_exception("failed to read file");

    auto checksum = nplex::crc32(snapshot.data(), snapshot.size());

    if (checksum != header.checksum)
        throw nplex::nplex_exception("checksum mismatch");

    return {header.rev, snapshot};
}

std::pair<nplex::rev_t, std::string> nplex::filesystem_snapshot_persister_t::read_snapshot(rev_t rev)
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

    return {0, ""};
}

void nplex::filesystem_snapshot_persister_t::write_snapshot_file(rev_t rev, const fs::path &path, std::string_view data)
{
    std::ofstream ofs(path, std::ios::binary | std::ios::trunc);
    if (!ofs)
        throw nplex_exception("failed to open snapshot file");

    auto checksum = nplex::crc32(data.data(), data.size());

    snapshot_file_header_t header(rev, checksum);

    ofs.write(reinterpret_cast<const char *>(&header), sizeof(header));
    ofs.write(data.data(), static_cast<std::streamsize>(data.size()));

    if (!ofs)
        throw nplex_exception("failed to write snapshot content");

    ofs.close();
}

void nplex::filesystem_snapshot_persister_t::write_snapshot(rev_t rev, std::string_view data)
{
    if (rev == 0 || data.empty())
        throw nplex_exception("snapshot payload is empty");

    auto filename = FILENAME_PREFIX + std::to_string(rev) + FILENAME_EXTENSION;

    try {
        write_snapshot_file(rev, m_path / filename, data);
    }
    catch (...) {
        LOG_ERROR("Failed to write file {}", filename);
        throw;
    }
}
