#include <doctest.h>
#include "journal_catalog.hpp"
#include <iostream>

using namespace std;
using namespace nplex;

std::string to_string(journal_catalog_builder_t::status_e status)
{
    switch (status) {
        case journal_catalog_builder_t::status_e::UNASSIGNED: return "UNASSIGNED";
        case journal_catalog_builder_t::status_e::ORPHAN: return "ORPHAN";
        case journal_catalog_builder_t::status_e::ACTIVE: return "ACTIVE";
        case journal_catalog_builder_t::status_e::UNUSED: return "UNUSED";
        case journal_catalog_builder_t::status_e::DUPLICATED: return "DUPLICATED";
        case journal_catalog_builder_t::status_e::EMPTY: return "EMPTY";
        case journal_catalog_builder_t::status_e::INVALID: return "INVALID";
        case journal_catalog_builder_t::status_e::HEAD: return "HEAD";
        default: return "UNKNOWN";
    }
}

void print_catalog(const journal_catalog_t &catalog)
{
    cout << "Catalog content:" << std::endl;

    for (const auto &[rev, segment] : catalog) {
        std::cout << "Rev: " << rev
                  << ", File: " << segment.file_name
                  << ", min_rev: " << segment.min_rev
                  << ", max_rev: " << segment.max_rev
                  << std::endl;
    }

    cout << std::endl;
}

class test_journal_catalog_builder_t : public journal_catalog_builder_t
{
  public:
    using journal_catalog_builder_t::mark_head;
    using journal_catalog_builder_t::mark_empties;
    using journal_catalog_builder_t::mark_duplicates;
    using journal_catalog_builder_t::mark_orphans_and_unuseds;
    using journal_catalog_builder_t::populate_files;
    using journal_catalog_builder_t::create_catalog;
    using journal_catalog_builder_t::create_links;
    using journal_catalog_builder_t::files;

    void clear() { m_files.clear(); }

    void append(const std::string &filename, rev_t min_rev, rev_t max_rev, status_e status = status_e::UNASSIGNED)
    {
        file_t file;

        file.file_name = filename;
        file.min_rev = min_rev;
        file.max_rev = max_rev;
        file.status = status;

        m_files.emplace(
            max_rev, 
            std::make_shared<file_t>(file)
        );
    }

    file_ptr get_file(const std::string &filename)
    {
        for (const auto &[rev, file] : m_files) {
            if (file->file_name == filename) {
                return file;
            }
        }

        return {};
    }

    std::vector<std::string> get_filenames_by_status(status_e status)
    {
        std::vector<std::string> filenames;

        for (const auto &[rev, file] : m_files) {
            if (file->status == status) {
                filenames.push_back(file->file_name);
            }
        }

        return filenames;
    }

    void print_files() const
    {
        std::cout << "Files content:" << std::endl;

        for (const auto &entry : m_files)
        {
            const auto &file = entry.second;
            std::cout << "File: " << file->file_name
                      << ", range=[" << file->min_rev << ".." << file->max_rev << "]"
                      << ", status=" << to_string(file->status)
                      << ", chain_min_rev=" << file->chain_min_rev()
                      << ", chain_num_segments=" << file->chain_num_segments()
                      << ", chain_num_overlaps=" << file->chain_num_overlaps()
                      << std::endl;

            for (size_t i = 0; i < entry.second->links.size(); ++i)
            {
                const auto &link = entry.second->links[i];

                std::cout << "  Link[" << i << "]: " << link.file->file_name
                          << ", num_segments: " << link.num_segments
                          << ", num_overlaps: " << link.num_overlaps
                          << ", min_rev: " << link.min_rev
                          << std::endl;
            }
        }

        cout << std::endl;
    }
};

TEST_CASE("journal_catalog_mark_empties")
{
    test_journal_catalog_builder_t builder;

    builder.clear();
    builder.append("journal-1.dat", 0, 0);
    builder.append("journal-2.dat", 10, 20);
    builder.append("journal-3.dat", 10, 20, journal_catalog_builder_t::status_e::INVALID);
    builder.append("journal-4.dat", 0, 0);
    builder.mark_empties();
    CHECK(builder.get_file("journal-1.dat")->status == journal_catalog_builder_t::status_e::EMPTY);
    CHECK(builder.get_file("journal-2.dat")->status == journal_catalog_builder_t::status_e::UNASSIGNED);
    CHECK(builder.get_file("journal-3.dat")->status == journal_catalog_builder_t::status_e::INVALID);
    CHECK(builder.get_file("journal-4.dat")->status == journal_catalog_builder_t::status_e::EMPTY);
}

TEST_CASE("journal_catalog_mark_duplicates")
{
    test_journal_catalog_builder_t builder;

    // No unassigned
    builder.clear();
    builder.append("journal-empty.dat", 10, 20, journal_catalog_builder_t::status_e::EMPTY);
    builder.append("journal-invalid.dat", 10, 20, journal_catalog_builder_t::status_e::INVALID);
    builder.mark_duplicates();
    CHECK(builder.get_filenames_by_status(journal_catalog_builder_t::status_e::DUPLICATED).empty());

    // Basic case
    builder.clear();
    builder.append("journal-b.dat", 10, 20);
    builder.append("journal-a.dat", 10, 20);
    builder.mark_duplicates();
    CHECK(builder.get_file("journal-a.dat")->status == journal_catalog_builder_t::status_e::UNASSIGNED);
    CHECK(builder.get_file("journal-b.dat")->status == journal_catalog_builder_t::status_e::DUPLICATED);

    // All cases (2 ranges)
    builder.clear();
    builder.append("journal1-b.dat", 10, 20); // journal-b to test lexicographic order
    builder.append("journal1-a.dat", 10, 20);
    builder.append("journal1-empty.dat", 10, 20, journal_catalog_builder_t::status_e::EMPTY);
    builder.append("journal1-invalid.dat", 10, 20, journal_catalog_builder_t::status_e::INVALID);
    builder.append("journal2-c.dat", 30, 40);
    builder.append("journal2-b.dat", 30, 40);
    builder.append("journal2-a.dat", 30, 40);
    builder.append("journal2-empty.dat", 30, 40, journal_catalog_builder_t::status_e::EMPTY);
    builder.append("journal2-invalid.dat", 30, 40, journal_catalog_builder_t::status_e::INVALID);
    builder.mark_duplicates();
    CHECK(builder.get_file("journal1-a.dat")->status == journal_catalog_builder_t::status_e::UNASSIGNED);
    CHECK(builder.get_file("journal1-b.dat")->status == journal_catalog_builder_t::status_e::DUPLICATED);
    CHECK(builder.get_file("journal1-empty.dat")->status == journal_catalog_builder_t::status_e::EMPTY);
    CHECK(builder.get_file("journal1-invalid.dat")->status == journal_catalog_builder_t::status_e::INVALID);
    CHECK(builder.get_file("journal2-a.dat")->status == journal_catalog_builder_t::status_e::UNASSIGNED);
    CHECK(builder.get_file("journal2-b.dat")->status == journal_catalog_builder_t::status_e::DUPLICATED);
    CHECK(builder.get_file("journal2-c.dat")->status == journal_catalog_builder_t::status_e::DUPLICATED);
    CHECK(builder.get_file("journal2-empty.dat")->status == journal_catalog_builder_t::status_e::EMPTY);
    CHECK(builder.get_file("journal2-invalid.dat")->status == journal_catalog_builder_t::status_e::INVALID);
}

TEST_CASE("journal_catalog_create_links")
{
    test_journal_catalog_builder_t builder;
    journal_catalog_builder_t::link_t link;
    journal_catalog_builder_t::file_ptr file;

    // No files
    builder.clear();
    builder.create_links();
    CHECK(builder.files().empty());

    // Simple file
    builder.clear();
    builder.append("journal.dat", 10, 20);
    builder.create_links();
    CHECK(builder.files().size() == 1);
    CHECK(builder.get_file("journal.dat")->links.empty());

    // Two files, no links
    builder.clear();
    builder.append("journal-a.dat", 10, 20);
    builder.append("journal-b.dat", 30, 40);
    builder.create_links();
    CHECK(builder.get_file("journal-a.dat")->links.empty());
    CHECK(builder.get_file("journal-b.dat")->links.empty());

    // Simple case (no overlap)
    builder.clear();
    builder.append("journal-a.dat", 10, 20);
    builder.append("journal-b.dat", 21, 25);
    builder.create_links();
    file = builder.get_file("journal-a.dat");
    CHECK(file->links.empty());
    CHECK(file->upstream() == nullptr);
    file = builder.get_file("journal-b.dat");
    CHECK(file->links.size() == 1);
    link = file->links[0];
    CHECK(link.file->file_name == "journal-a.dat");
    CHECK(link.num_segments == 1);
    CHECK(link.num_overlaps == 0);
    CHECK(file->upstream()->file_name == "journal-a.dat");

    // Simple case (with overlap and overlap allowed)
    builder.clear();
    builder.with_overlap();
    builder.append("journal-a.dat", 10, 20);
    builder.append("journal-b.dat", 15, 25);
    builder.create_links();
    file = builder.get_file("journal-a.dat");
    CHECK(file->links.empty());
    CHECK(file->upstream() == nullptr);
    file = builder.get_file("journal-b.dat");
    CHECK(file->links.size() == 1);
    link = file->links[0];
    CHECK(link.file->file_name == "journal-a.dat");
    CHECK(link.num_segments == 1);
    CHECK(link.num_overlaps == 6);
    CHECK(file->upstream()->file_name == "journal-a.dat");

    // Simple case (with overlap and overlap not allowed)
    builder.clear();
    builder.with_overlap(false);
    builder.append("journal-a.dat", 10, 20);
    builder.append("journal-b.dat", 15, 25);
    builder.create_links();
    file = builder.get_file("journal-a.dat");
    CHECK(file->links.empty());
    CHECK(file->upstream() == nullptr);
    file = builder.get_file("journal-b.dat");
    CHECK(file->links.empty());
    CHECK(file->upstream() == nullptr);

    // Multiple links
    builder.clear();
    builder.with_overlap();
    builder.append("journal-a.dat", 10, 20);
    builder.append("journal-b.dat", 15, 25);
    builder.append("journal-c.dat", 18, 30);
    builder.create_links();
    file = builder.get_file("journal-a.dat");
    CHECK(file->links.empty());
    CHECK(file->upstream() == nullptr);
    file = builder.get_file("journal-b.dat");
    CHECK(file->links.size() == 1);
    link = file->links[0];
    CHECK(link.file->file_name == "journal-a.dat");
    CHECK(link.num_segments == 1);
    CHECK(link.num_overlaps == 6);
    CHECK(file->upstream()->file_name == "journal-a.dat");
    file = builder.get_file("journal-c.dat");
    CHECK(file->links.size() == 2);
    link = file->links[0];
    CHECK(link.file->file_name == "journal-a.dat");
    CHECK(link.num_segments == 1);
    CHECK(link.num_overlaps == 3);
    link = file->links[1];
    CHECK(link.file->file_name == "journal-b.dat");
    CHECK(link.num_segments == 2);
    CHECK(link.num_overlaps == 8 + 6);
    CHECK(file->upstream()->file_name == "journal-a.dat");

    // Long chain (3 segments)
    builder.clear();
    builder.with_overlap();
    builder.append("journal-0.dat", 1, 9);
    builder.append("journal-a.dat", 10, 20);
    builder.append("journal-b.dat", 15, 25);
    builder.append("journal-c.dat", 18, 30);
    builder.append("journal-invalid.dat", 12, 27, journal_catalog_builder_t::status_e::INVALID);
    builder.append("journal-empty.dat", 0, 0, journal_catalog_builder_t::status_e::EMPTY);
    builder.create_links();
    file = builder.get_file("journal-0.dat");
    CHECK(file->links.empty());
    CHECK(file->upstream() == nullptr);
    file = builder.get_file("journal-a.dat");
    CHECK(file->links.size() == 1);
    CHECK(file->upstream()->file_name == "journal-0.dat");
    file = builder.get_file("journal-b.dat");
    CHECK(file->links.size() == 1);
    link = file->links[0];
    CHECK(link.file->file_name == "journal-a.dat");
    CHECK(link.num_segments == 2);
    CHECK(link.num_overlaps == 6);
    CHECK(file->upstream()->file_name == "journal-a.dat");
    file = builder.get_file("journal-c.dat");
    CHECK(file->links.size() == 2);
    link = file->links[0];
    CHECK(link.file->file_name == "journal-a.dat");
    CHECK(link.num_segments == 2);
    CHECK(link.num_overlaps == 3);
    link = file->links[1];
    CHECK(link.file->file_name == "journal-b.dat");
    CHECK(link.num_segments == 3);
    CHECK(link.num_overlaps == 8 + 6);
    CHECK(file->upstream()->file_name == "journal-a.dat");
}

TEST_CASE("journal_catalog_mark_head_no_range")
{
    test_journal_catalog_builder_t builder;

    // Simple file
    builder.clear();
    builder.with_range(0, 0);
    builder.with_overlap();
    builder.append("journal.dat", 10, 20);
    builder.create_links();
    builder.mark_head();
    CHECK(builder.get_file("journal.dat")->status == journal_catalog_builder_t::status_e::HEAD);

    // Two files, with_overlap
    builder.clear();
    builder.with_range(0, 0);
    builder.with_overlap();
    builder.append("journal-1.dat", 10, 19);
    builder.append("journal-2.dat", 20, 30);
    builder.create_links();
    builder.mark_head();
    CHECK(builder.get_file("journal-1.dat")->status == journal_catalog_builder_t::status_e::UNASSIGNED);
    CHECK(builder.get_file("journal-2.dat")->status == journal_catalog_builder_t::status_e::HEAD);

    // Two files, with_overlap
    builder.clear();
    builder.with_range(0, 0);
    builder.with_overlap();
    builder.append("journal-1.dat", 10, 25);
    builder.append("journal-2.dat", 20, 30);
    builder.create_links();
    builder.mark_head();
    CHECK(builder.get_file("journal-1.dat")->status == journal_catalog_builder_t::status_e::UNASSIGNED);
    CHECK(builder.get_file("journal-2.dat")->status == journal_catalog_builder_t::status_e::HEAD);

    // Two files, no_overlap
    builder.clear();
    builder.with_range(0, 0);
    builder.with_overlap(false);
    builder.append("journal-1.dat", 10, 19);
    builder.append("journal-2.dat", 20, 30);
    builder.create_links();
    builder.mark_head();
    CHECK(builder.get_file("journal-1.dat")->status == journal_catalog_builder_t::status_e::UNASSIGNED);
    CHECK(builder.get_file("journal-2.dat")->status == journal_catalog_builder_t::status_e::HEAD);

    // Two files, no_overlap
    builder.clear();
    builder.with_range(0, 0);
    builder.with_overlap(false);
    builder.append("journal-1.dat", 10, 25);
    builder.append("journal-2.dat", 20, 30);
    builder.create_links();
    builder.mark_head();
    CHECK(builder.get_file("journal-1.dat")->status == journal_catalog_builder_t::status_e::UNASSIGNED);
    CHECK(builder.get_file("journal-2.dat")->status == journal_catalog_builder_t::status_e::HEAD);

    // Complex, with overlap
    builder.clear();
    builder.with_range(0, 0);
    builder.with_overlap();
    builder.append("journal-1.dat", 20, 30);
    builder.append("journal-2.dat", 25, 50);
    builder.append("journal-3.dat", 51, 70);  // Total chain range = 20..70
    builder.append("journal-4.dat", 15, 70);  // Total chain range = 15..70 <- wins
    builder.create_links();
    builder.mark_head();
    CHECK(builder.get_file("journal-4.dat")->status == journal_catalog_builder_t::status_e::HEAD);

    // Complex, with overlap
    builder.clear();
    builder.with_range(0, 0);
    builder.with_overlap();
    builder.append("journal-1.dat", 20, 30);
    builder.append("journal-2.dat", 31, 50);
    builder.append("journal-3.dat", 51, 70); // Total chain range = 20..70, num_segments = 3
    builder.append("journal-4.dat", 21, 70); // Total chain range = 20..70, num_segments = 2 <- wins
    builder.create_links();
    builder.mark_head();
    CHECK(builder.get_file("journal-4.dat")->status == journal_catalog_builder_t::status_e::HEAD);

    // Complex, with overlap
    builder.clear();
    builder.with_range(0, 0);
    builder.with_overlap();
    builder.append("journal-1.dat", 20, 30);
    builder.append("journal-2.dat", 31, 50);
    builder.append("journal-3.dat", 51, 70); // Total chain range = 20..70, num_segments = 3, num_overlaps = 0  <- wins
    builder.append("journal-4.dat", 40, 70); // Total chain range = 20..70, num_segments = 3, num_overlaps = 11
    builder.create_links();
    builder.mark_head();
    CHECK(builder.get_file("journal-3.dat")->status == journal_catalog_builder_t::status_e::HEAD);
}

TEST_CASE("journal_catalog_mark_head_with_range")
{
    test_journal_catalog_builder_t builder;

    // Simple file, no_match
    builder.clear();
    builder.with_overlap();
    builder.with_range(10, 20);
    builder.append("journal.dat", 1, 5);
    builder.create_links();
    builder.mark_head();
    CHECK(builder.get_file("journal.dat")->status == journal_catalog_builder_t::status_e::UNASSIGNED);

    // Simple file, no_match
    builder.clear();
    builder.with_overlap();
    builder.with_range(10, 20);
    builder.append("journal.dat", 30, 40);
    builder.create_links();
    builder.mark_head();
    CHECK(builder.get_file("journal.dat")->status == journal_catalog_builder_t::status_e::UNASSIGNED);

    // Simple file, concatenate
    builder.clear();
    builder.with_overlap();
    builder.with_range(10, 20);
    builder.append("journal.dat", 5, 9);
    builder.create_links();
    builder.mark_head();
    CHECK(builder.get_file("journal.dat")->status == journal_catalog_builder_t::status_e::HEAD);

    // Simple file, with_overlap
    builder.clear();
    builder.with_overlap();
    builder.with_range(10, 20);
    builder.append("journal.dat", 5, 12);
    builder.create_links();
    builder.mark_head();
    CHECK(builder.get_file("journal.dat")->status == journal_catalog_builder_t::status_e::HEAD);

    // Simple file, with_range, included
    builder.clear();
    builder.with_overlap();
    builder.with_range(10, 20);
    builder.append("journal.dat", 12, 18);
    builder.create_links();
    builder.mark_head();
    CHECK(builder.get_file("journal.dat")->status == journal_catalog_builder_t::status_e::UNASSIGNED);

    // Simple file, with_range, surpass
    builder.clear();
    builder.with_overlap();
    builder.with_range(10, 20);
    builder.append("journal.dat", 15, 25);
    builder.create_links();
    builder.mark_head();
    CHECK(builder.get_file("journal.dat")->status == journal_catalog_builder_t::status_e::UNASSIGNED);
}

TEST_CASE("journal_catalog_mark_orphans_and_unuseds")
{
    test_journal_catalog_builder_t builder;

    builder.clear();
    builder.append("journal-empty.dat", 0, 0, journal_catalog_builder_t::status_e::EMPTY);
    builder.append("journal-invalid.dat", 10, 20, journal_catalog_builder_t::status_e::INVALID);
    builder.append("journal-head.dat", 21, 30, journal_catalog_builder_t::status_e::HEAD);
    builder.append("journal-duplicate.dat", 10, 20, journal_catalog_builder_t::status_e::DUPLICATED);
    builder.append("journal-active.dat", 10, 20, journal_catalog_builder_t::status_e::ACTIVE);
    builder.append("journal-1.dat",  5,  8);
    builder.append("journal-2.dat", 12, 18);
    builder.append("journal-3.dat", 15, 25);
    builder.append("journal-4.dat",  5, 15);
    builder.append("journal-5.dat", 25, 35);

    builder.mark_orphans_and_unuseds(10);

    CHECK(builder.get_file("journal-empty.dat")->status == journal_catalog_builder_t::status_e::EMPTY);
    CHECK(builder.get_file("journal-invalid.dat")->status == journal_catalog_builder_t::status_e::INVALID);
    CHECK(builder.get_file("journal-head.dat")->status == journal_catalog_builder_t::status_e::HEAD);
    CHECK(builder.get_file("journal-duplicate.dat")->status == journal_catalog_builder_t::status_e::DUPLICATED);
    CHECK(builder.get_file("journal-active.dat")->status == journal_catalog_builder_t::status_e::ACTIVE);
    CHECK(builder.get_file("journal-1.dat")->status == journal_catalog_builder_t::status_e::ORPHAN);
    CHECK(builder.get_file("journal-2.dat")->status == journal_catalog_builder_t::status_e::UNUSED);
    CHECK(builder.get_file("journal-3.dat")->status == journal_catalog_builder_t::status_e::UNUSED);
    CHECK(builder.get_file("journal-4.dat")->status == journal_catalog_builder_t::status_e::UNUSED);
    CHECK(builder.get_file("journal-5.dat")->status == journal_catalog_builder_t::status_e::UNUSED);
}
