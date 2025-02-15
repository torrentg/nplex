#pragma once

#include <vector>
#include <filesystem>

// Forward declaration
struct ldb_impl_t;
typedef struct ldb_impl_t ldb_journal_t;

namespace nplex {

/**
 * Wrapper on journal code.
 * 
 * This class ensures that a journal is properly allocated, opened, and closed
 * using the RAII idiom. It prevents resource leaks by managing the lifecycle
 * of the journal resource.
 * 
 * The class is default-constructible, non-copyable and movable.
 */
class journal_t
{
  public:

    journal_t() = default;
    journal_t(const std::filesystem::path &path, bool check = true);
    ~journal_t();

    journal_t(const journal_t&) = delete;
    journal_t & operator=(const journal_t&) = delete;

    journal_t(journal_t&& other) noexcept : journal_t() {
        swap(*this, other);
    }

    journal_t & operator=(journal_t&& other) noexcept {
        swap(*this, other);
        return *this;
    }

    friend void swap(journal_t& first, journal_t& second) noexcept {
        using std::swap;
        swap(first.m_journal, second.m_journal);
    }

    void set_fsync(bool enable);

    // void append(std::vector<std::string> &entries);
    // void read();
    // void stats();
    // void find();
    // void rollback();
    // void purge();

  private:
    ldb_journal_t *m_journal = nullptr;
};

} // namespace nplex
