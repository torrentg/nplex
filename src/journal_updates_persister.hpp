#pragma once

#include "persister.hpp"
#include <filesystem>

// Forward declarations
namespace ldb {
    class journal_t;
}

namespace nplex {

/**
 * Persister that stores updates in a journal format.
 * 
 * @see https://github.com/torrentg/journal
 */
class journal_updates_persister_t : public updates_persister_t
{
  public:

    using journal_ptr = std::shared_ptr<ldb::journal_t>;

    journal_updates_persister_t(const std::filesystem::path &path);
    ~journal_updates_persister_t() override = default;

    journal_updates_persister_t(const journal_updates_persister_t &) = delete;
    journal_updates_persister_t &operator=(const journal_updates_persister_t &) = delete;
    journal_updates_persister_t(journal_updates_persister_t &&) = delete;
    journal_updates_persister_t &operator=(journal_updates_persister_t &&) = delete;

    std::pair<nplex::rev_t, nplex::rev_t> get_updates_range() override;
    void read_updates(rev_t from_rev, const read_callback_f &on_entry, std::size_t max_entries = UINT64_MAX) override;
    void write_updates(const std::span<update_entry_t> &entries, std::size_t &num_writes) override;

    static journal_ptr open_journal_file(const std::filesystem::path &path, const std::string &name, int flags);
    static journal_ptr open_journal_file(const std::filesystem::path &filepath, int flags);

  private:

    std::filesystem::path m_path;
    journal_ptr m_journal;
};

} // namespace nplex
