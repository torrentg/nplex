#include "filesystem_snapshot_persister.hpp"
#include "buildinfo.hpp"
#include "exception.hpp"
#include "schema.hpp"
#include "utils.hpp"
#include <charconv>
#include <filesystem>
#include <fstream>
#include <string_view>

namespace fs = std::filesystem;

// ==========================================================
// Internal (static) functions
// ==========================================================

namespace {

constexpr std::string_view FILENAME_PREFIX = "snapshot-";
constexpr std::string_view FILENAME_EXTENSION = ".dat";
constexpr std::string_view MAGIC_BYTES = "NPLEXSNP";

struct snapshot_file_header_t
{
    std::uint8_t magic[8];
    std::uint32_t schema2_hash;
    std::uint32_t checksum;

    explicit snapshot_file_header_t(std::uint32_t chk = 0) : schema2_hash(SCHEMA2_HASH), checksum(chk)
    {
        std::memcpy(magic, MAGIC_BYTES.data(), sizeof(magic));
    }
};

fs::path snapshot_path_for_rev(const fs::path &directory, nplex::rev_t rev)
{
    return directory / fmt::format("{}{}{}", FILENAME_PREFIX, rev, FILENAME_EXTENSION);
}

nplex::rev_t parse_snapshot_rev(const fs::path &path)
{
    const std::string filename = path.filename().string();

    if (!filename.starts_with(FILENAME_PREFIX))
        return 0;

    if (!filename.ends_with(FILENAME_EXTENSION))
        return 0;

    const auto rev_text = std::string_view{
        filename.data() + FILENAME_PREFIX.size(),
        filename.size() - FILENAME_PREFIX.size() - FILENAME_EXTENSION.size()};

    if (rev_text.empty())
        return 0;

    nplex::rev_t rev = 0;
    const char *begin = rev_text.data();
    const char *end = begin + rev_text.size();
    const auto [ptr, ec] = std::from_chars(begin, end, rev);

    if (ec != std::errc() || ptr != end)
        return 0;

    return rev;
}

void validate_snapshot_payload(const std::string_view &data)
{
    auto verifier = flatbuffers::Verifier(
        reinterpret_cast<const std::uint8_t *>(data.data()), data.size());

    if (!verifier.VerifyBuffer<nplex::msgs::Snapshot>(nullptr))
        throw nplex::nplex_exception("invalid snapshot payload");
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
        throw nplex_exception("snapshot directory does not exist or is not a directory: {}", m_path.string());

    const auto perms = fs::status(m_path, ec).permissions();

    if (ec)
        throw nplex_exception("unable to access snapshot directory: {}", m_path.string());

    if ((perms & fs::perms::owner_read) == fs::perms::none &&
        (perms & fs::perms::group_read) == fs::perms::none &&
        (perms & fs::perms::others_read) == fs::perms::none)
    {
        throw nplex_exception("snapshot directory is not readable: {}", m_path.string());
    }
}

std::pair<nplex::rev_t, nplex::rev_t> nplex::filesystem_snapshot_persister_t::get_snapshots_range()
{
    std::pair<nplex::rev_t, nplex::rev_t> range{0, 0};
    bool found = false;

    if (!fs::exists(m_path))
        return range;

    for (const auto &entry : fs::directory_iterator(m_path))
    {
        if (!entry.is_regular_file())
            continue;

        const auto rev = parse_snapshot_rev(entry.path());
        if (rev == 0)
            continue;

        if (!found)
        {
            range.first = rev;
            range.second = rev;
            found = true;
            continue;
        }

        if (rev < range.first)
            range.first = rev;

        if (rev > range.second)
            range.second = rev;
    }

    return range;
}

std::string nplex::filesystem_snapshot_persister_t::read_snapshot_file(const std::filesystem::path &path)
{
    if (!fs::exists(path))
        return {};

    std::ifstream ifs(path, std::ios::binary);
    if (!ifs)
        throw nplex::nplex_exception("failed to open snapshot file ({})", path.string());

    ifs.seekg(0, std::ios::end);
    if (!ifs)
        throw nplex::nplex_exception("failed to seek snapshot file ({})", path.string());

    const auto file_size = ifs.tellg();
    if (file_size < static_cast<std::streamoff>(sizeof(snapshot_file_header_t)))
        throw nplex::nplex_exception("invalid snapshot file ({})", path.string());

    ifs.seekg(0, std::ios::beg);
    if (!ifs)
        throw nplex::nplex_exception("failed to rewind snapshot file ({})", path.string());

    snapshot_file_header_t header{};

    ifs.read(reinterpret_cast<char *>(&header), sizeof(header));
    if (!ifs || std::memcmp(header.magic, MAGIC_BYTES.data(), MAGIC_BYTES.size()) != 0)
        throw nplex::nplex_exception("invalid snapshot header ({})", path.string());

    if (header.schema2_hash != SCHEMA2_HASH)
        throw nplex::nplex_exception("snapshot schema mismatch ({})", path.string());

    const auto payload_size = static_cast<std::size_t>(file_size) - sizeof(header);
    std::string payload(payload_size, '\0');

    ifs.read(payload.data(), static_cast<std::streamsize>(payload_size));
    if (!ifs)
        throw nplex::nplex_exception("failed to read snapshot payload ({})", path.string());

    const auto expected = header.checksum;
    const auto actual = nplex::crc32(payload.data(), payload.size());

    if (actual != expected)
        throw nplex::nplex_exception("snapshot checksum mismatch ({})", path.string());

    validate_snapshot_payload(payload);

    return payload;
}
    
std::string nplex::filesystem_snapshot_persister_t::read_snapshot(rev_t rev)
{
    nplex::rev_t best_rev = 0;
    fs::path best_file;

    for (const auto &entry : fs::directory_iterator(m_path))
    {
        if (!entry.is_regular_file())
            continue;

        auto candidate_rev = parse_snapshot_rev(entry.path());

        if (candidate_rev == 0)
            continue;

        if (candidate_rev > rev)
            continue;

        if (candidate_rev > best_rev)
        {
            best_rev = candidate_rev;
            best_file = entry.path();
        }
    }

    if (best_rev == 0)
        return {};

    return read_snapshot_file(best_file);
}

void nplex::filesystem_snapshot_persister_t::write_snapshot_file(const std::filesystem::path &path, const std::string_view &data)
{
    std::ofstream ofs(path, std::ios::binary | std::ios::trunc);
    if (!ofs)
        throw nplex_exception("failed to open snapshot file ({})", path.string());

    auto checksum = nplex::crc32(data.data(), data.size());
    snapshot_file_header_t header(checksum);

    ofs.write(reinterpret_cast<const char *>(&header), sizeof(header));
    if (!ofs)
        throw nplex_exception("failed to write snapshot header ({})", path.string());

    ofs.write(data.data(), static_cast<std::streamsize>(data.size()));
    if (!ofs)
        throw nplex_exception("failed to write snapshot payload ({})", path.string());

    ofs.close();
    if (!ofs)
        throw nplex_exception("failed to flush snapshot file ({})", path.string());
}

void nplex::filesystem_snapshot_persister_t::write_snapshot(rev_t rev, const std::string_view &data)
{
    if (rev == 0 || data.empty())
        throw nplex_exception("snapshot payload is empty");

    validate_snapshot_payload(data);

    auto snapshot = flatbuffers::GetRoot<nplex::msgs::Snapshot>(data.data());
    if (snapshot->rev() != rev)
        throw nplex_exception("snapshot revision mismatch: file rev {} != payload rev {}", rev, snapshot->rev());

    auto filepath = snapshot_path_for_rev(m_path, rev);

    write_snapshot_file(filepath, data);
}
