#include <string>
#include <stdexcept>
#include "journal.h"
#include "nplex.hpp"
#include "journal.hpp"

nplex::journal_t::journal_t(const std::filesystem::path &path, bool check)
{
    m_journal = ldb_alloc();

    if (!m_journal)
        throw std::bad_alloc();

    int rc = ldb_open(m_journal, path.c_str(), PROJECT_NAME, check);

    if (rc != LDB_OK) {
        throw std::runtime_error(ldb_strerror(rc));
    }
}

nplex::journal_t::~journal_t()
{
    if (m_journal) {
        ldb_close(m_journal);
        ldb_free(m_journal);
        m_journal = nullptr;
    }
}

void nplex::journal_t::set_fsync(bool enable)
{
    if (m_journal)
        ldb_set_fsync(m_journal, enable);
}
