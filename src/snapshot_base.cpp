#include "snapshot_base.hpp"
#include "buildinfo.hpp"
#include "exception.hpp"
#include "logger.hpp"
#include "utils.hpp"
#include <fstream>
#include <cstring>

namespace fs = std::filesystem;

// ==========================================================
// Internal (static) functions
// ==========================================================

namespace {

constexpr std::string MAGIC_BYTES = "NPLEXSNP";

bool is_valid_header(const nplex::snapshot_file_header_t &header)
{
    if (std::memcmp(header.magic, MAGIC_BYTES.data(), sizeof(header.magic)) != 0)
        return false;

    if (header.schema2_hash != SCHEMA2_HASH)
        return false;

    if (header.rev == 0)
        return false;

    return true;
}

} // unnamed namespace

// ==========================================================
// nplex journal functions
// ==========================================================

nplex::snapshot_file_header_t nplex::read_snapshot_file_header(const fs::path &path)
{
    snapshot_file_header_t header{};
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

std::tuple<nplex::rev_t, nplex::millis_t, std::string> nplex::read_snapshot_file(const fs::path &path)
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

    return {header.rev, millis_t{header.timestamp}, snapshot};
}

void nplex::write_snapshot_file(rev_t rev, millis_t timestamp, const fs::path &path, std::string_view data)
{
    std::ofstream ofs(path, std::ios::binary | std::ios::trunc);
    if (!ofs)
        throw nplex_exception("failed to open snapshot file");

    auto checksum = nplex::crc32(data.data(), data.size());
    auto ts = static_cast<std::uint64_t>(timestamp.count());

    snapshot_file_header_t header{
        .schema2_hash = SCHEMA2_HASH,
        .checksum = checksum,
        .rev = rev,
        .timestamp = ts
    };

    memcpy(&header.magic, MAGIC_BYTES.data(), sizeof(header.magic));

    ofs.write(reinterpret_cast<const char *>(&header), sizeof(header));
    ofs.write(data.data(), static_cast<std::streamsize>(data.size()));

    if (!ofs)
        throw nplex_exception("failed to write snapshot content");

    ofs.close();
}
