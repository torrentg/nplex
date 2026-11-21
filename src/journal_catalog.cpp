#include "journal_catalog.hpp"
#include "exception.hpp"
#include "logger.hpp"
#include "journal.h"
#include <cassert>

namespace fs = std::filesystem;

// ==========================================================
// Internal (static) functions
// ==========================================================

namespace {

struct file_cmp
{
    bool operator()(const nplex::journal_catalog_builder_t::file_ptr &lhs, const nplex::journal_catalog_builder_t::file_ptr &rhs) const {
        if (lhs->min_rev == rhs->min_rev)
            if (lhs->max_rev == rhs->max_rev)
                return (lhs->file_name < rhs->file_name);
            else
                return (lhs->max_rev < rhs->max_rev);
        else
            return (lhs->min_rev < rhs->min_rev);
    }
};

struct link_cmp
{
    bool operator()(const nplex::journal_catalog_builder_t::link_t &lhs, const nplex::journal_catalog_builder_t::link_t &rhs) const {
        if (lhs.num_segments != rhs.num_segments)
            return (lhs.num_segments < rhs.num_segments);
        return (lhs.num_overlaps < rhs.num_overlaps);
    }
};

struct chain_info_t
{
    nplex::rev_t min_rev = 0;
    std::size_t num_segments = 0;
    std::size_t num_overlaps = 0;
};

bool is_better_chain(const chain_info_t &file, const chain_info_t &head)
{
    if (file.min_rev > head.min_rev)
        return false;

    if (file.min_rev < head.min_rev)
        return true;

    if (file.num_segments > head.num_segments)
        return false;

    if (file.num_segments < head.num_segments)
        return true;

    if (file.num_overlaps > head.num_overlaps)
        return false;

    if (file.num_overlaps < head.num_overlaps)
        return true;

    return false;
}

nplex::journal_catalog_builder_t::file_ptr create_file(const fs::path &filepath)
{
    try
    {
        return std::make_shared<nplex::journal_catalog_builder_t::file_t>(filepath);
    }
    catch(const std::exception &e)
    {
        LOG_WARN("Failed to open {}: {}", filepath.filename().string(), e.what());
        auto file = std::make_shared<nplex::journal_catalog_builder_t::file_t>();
        file->file_name = filepath.filename().string();
        file->status = nplex::journal_catalog_builder_t::status_e::INVALID;
        return file;
    }
}

} // unnamed namespace

// ==========================================================
// journal_catalog_builder_t methods
// ==========================================================

nplex::journal_catalog_builder_t& nplex::journal_catalog_builder_t::with_path(const fs::path &path) {
    m_path = path;
    return *this;
}

nplex::journal_catalog_builder_t& nplex::journal_catalog_builder_t::with_files_prefix(const std::string &prefix) {
    m_files_prefix = prefix;
    return *this;
}

nplex::journal_catalog_builder_t& nplex::journal_catalog_builder_t::with_files_extension(const std::string &ext) {
    m_files_extension = ext;
    return *this;
}

nplex::journal_catalog_builder_t& nplex::journal_catalog_builder_t::with_range(rev_t first_rev, rev_t last_rev) {
    assert(first_rev <= last_rev);
    m_range = {first_rev, last_rev};
    return *this;
}

nplex::journal_catalog_builder_t& nplex::journal_catalog_builder_t::with_range(std::pair<rev_t, rev_t> range) {
    assert(range.first <= range.second);
    m_range = range;
    return *this;
}

nplex::journal_catalog_builder_t& nplex::journal_catalog_builder_t::with_overlap(bool overlap) {
    m_overlap_allowed = overlap;
    return *this;
}

nplex::journal_catalog_t nplex::journal_catalog_builder_t::build(const journal_catalog_t &prev_catalog)
{
    if (m_range.first > m_range.second)
        throw nplex_exception("invalid range");

    populate_files(prev_catalog);
    mark_empties();
    mark_duplicates();
    create_links();
    mark_head();

    auto catalog = create_catalog();

    rev_t catalog_min_rev = (catalog.empty() ? 0 : catalog.begin()->second.min_rev);

    mark_orphans_and_unuseds(catalog_min_rev);

    return catalog;
}

void nplex::journal_catalog_builder_t::mark_head()
{
    bool search_by_max_update = (m_range.first == 0 || m_range.second == 0);
    chain_info_t head_chain_info;
    file_ptr head = nullptr;

    for (auto it = m_files.begin(); it != m_files.end(); ++it)
    {
        auto &file = it->second;

        if (file->status != status_e::UNASSIGNED)
            continue;

        chain_info_t file_chain_info = {
            .min_rev = file->chain_min_rev(),
            .num_segments = file->chain_num_segments(),
            .num_overlaps = file->chain_num_overlaps()
        };

        if (search_by_max_update)
        {
            if (!head) {
                head = file;
                head_chain_info = file_chain_info;
                continue;
            }

            if (file->max_rev < head->max_rev)
                continue;

            // Select if it contains more recent revisions than current head
            if (file->max_rev > head->max_rev) {
                head = file;
                head_chain_info = file_chain_info;
                continue;
            }

            // Head chain covers more revisions with fewer segments and overlaps
            if (is_better_chain(file_chain_info, head_chain_info)) {
                head_chain_info = file_chain_info;
                head = file;
                continue;
            }
        }
        else
        {
            if (file->max_rev + 1 < m_range.first)
                continue;

            if (file->max_rev > m_range.second)
                continue;

            if (file->min_rev >= m_range.first)
                continue;

            std::size_t num_overlaps = file->max_rev - m_range.first + 1;

            if (!m_overlap_allowed && num_overlaps > 0)
                continue;

            // Adding the overlap with the current journal (range)
            file_chain_info.num_overlaps += num_overlaps;

            // Head chain covers more revisions with fewer segments and overlaps
            if (!head || is_better_chain(file_chain_info, head_chain_info)) {
                head_chain_info = file_chain_info;
                head = file;
                continue;
            }
        }
    }

    if (head)
        head->status = status_e::HEAD;

    return;
}

nplex::journal_catalog_t nplex::journal_catalog_builder_t::create_catalog()
{
    journal_catalog_t catalog;

    // Search the HEAD file
    const auto it = std::ranges::find_if(
        m_files, 
        [](const auto &item) {
            return item.second->status == status_e::HEAD;
        }
    );

    if(it == m_files.end())
        return {};

    auto upstream = it->second;

    // Iterate backwards to recover the full chain.
    while (upstream)
    {
        upstream->status = status_e::ACTIVE;

        catalog.emplace(
            upstream->max_rev,
            static_cast<journal_segment_t&>(*upstream)
        );

        upstream = upstream->upstream();
    }

    return catalog;
}

void nplex::journal_catalog_builder_t::mark_orphans_and_unuseds(rev_t rev)
{
    for ([[maybe_unused]] auto &[xxx, file] : m_files)
    {
        if (file->status != status_e::UNASSIGNED)
            continue;

        if (file->max_rev + 1 < rev)
            file->status = status_e::ORPHAN;
        else
            file->status = status_e::UNUSED;
    }
}

void nplex::journal_catalog_builder_t::mark_empties()
{
    for ([[maybe_unused]] auto &[rev, file] : m_files)
    {
        if (file->status != status_e::UNASSIGNED)
            continue;

        if (file->min_rev == 0 || file->max_rev == 0)
            file->status = status_e::EMPTY;
    }
}

void nplex::journal_catalog_builder_t::populate_files(const journal_catalog_t &prev_catalog)
{
    if (m_path.empty() || m_files_prefix.empty() || m_files_extension.empty())
        throw nplex_exception("invalid configuration");

    std::map<std::string, const journal_segment_t &> prev_files;

    for (const auto &[rev, segment] : prev_catalog)
        prev_files.emplace(segment.file_name, segment);

    // Scan the directory for new journal files
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
        auto it = prev_files.find(file_name);

        if (it != prev_files.end())
        {
            // Create a journal file entry from previous catalog
            m_files.emplace(
                it->second.max_rev, 
                std::make_shared<file_t>(it->second)
            );

            continue;
        }

        // Create a new journal file entry
        auto file = ::create_file(entry.path());
        assert(file != nullptr);

        m_files.emplace(
            file->max_rev,
            file
        );
    }
}

void nplex::journal_catalog_builder_t::mark_duplicates()
{
    // Multiple files may share the same revision range (min_rev, max_rev).
    // Additionally, some of them may be invalid. One valid file is selected 
    // as canonical; the remaining valid files are marked as DUPLICATED.

    for (auto it1 = m_files.begin(); it1 != m_files.end();)
    {
        auto last = it1;
        auto representative = m_files.end();

        // Identify files having same max_rev and its representative
        while (last != m_files.end() && last->first == it1->first)
        {
            // Representative is among unassigned files
            if (last->second->status == status_e::UNASSIGNED)
            {
                // First valid unassigned file becomes the representative
                if (representative == m_files.end())
                    representative = last;
                // Prefer the lexicographically smaller representative name
                else if (representative->second->file_name > last->second->file_name)
                    representative = last;
            }

            last = std::next(last);
        }

        // Mark duplicates among files with the same range
        for (auto it2 = it1; it2 != last; ++it2)
        {
            if (it2 == representative)
                continue;

            if (it2->second->status != status_e::UNASSIGNED)
                continue;

            it2->second->status = status_e::DUPLICATED;
        }

        it1 = last;
    }
}

void nplex::journal_catalog_builder_t::create_links()
{
    std::vector<file_ptr> sorted_list;

    for ([[maybe_unused]] auto &[rev,file] : m_files) {
        if (file->status == status_e::UNASSIGNED)
            sorted_list.push_back(file);
    }

    // Sort valid files by their minimum revision number
    std::sort(sorted_list.begin(), sorted_list.end(), ::file_cmp{});

    // Searching for upstream candidates
    for (const auto &file : sorted_list)
    {
        assert(file->min_rev > 0);

        for (auto it = m_files.lower_bound(file->min_rev - 1); it != m_files.end(); ++it)
        {
            auto &candidate = it->second;

            if (candidate->status != status_e::UNASSIGNED)
                continue;

            if (candidate->max_rev >= file->max_rev)
                break;

            if (candidate->min_rev >= file->min_rev)
                continue;

            if (!m_overlap_allowed && candidate->max_rev + 1 != file->min_rev)
                continue;

            if (candidate->max_rev + 1 < file->min_rev)
                continue;

            link_t link {
                .file = candidate,
                .num_segments = 1,
                .num_overlaps = static_cast<std::uint32_t>(candidate->max_rev + 1 - file->min_rev),
                .min_rev = candidate->min_rev
            };

            file->links.push_back(link);
        }
    }

    // Select the 'optimal' upstream from the links list
    for (const auto &file : sorted_list)
    {
        // Sort links (less is better, upstream set at position 0)
        std::sort(file->links.begin(), file->links.end(), ::link_cmp{});

        // Update links values
        for (auto &link : file->links) {
            if (!link.file->links.empty()) {
                const auto &prev = link.file->links[0];
                link.num_overlaps += prev.num_overlaps;
                link.num_segments += prev.num_segments;
                link.min_rev = prev.min_rev;
            }
        }
    }
}
