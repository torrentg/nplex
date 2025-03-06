#include <mutex>
#include <memory>
#include <functional>
#include <condition_variable>
#include "flatbuffers/flatbuffers.h"
#include "cqueue.hpp"
#include "journal.h"
#include "exception.hpp"
#include "params.hpp"
#include "common.hpp"

namespace nplex {

/**
 * Object writing entries into the journal.
 * 
 * This object runs on a dedicated thread.
 * Write failure stops the writer thread.
 */
class writer_t
{
  public:

    using callback_t = std::function<void(bool, const std::vector<update_t> &&)>;
    using journal_ptr = std::shared_ptr<ldb::journal_t>;

    writer_t(const params_t &params, const journal_ptr &journal, callback_t callback);

    void write(update_t &&upd);
    void stop();
    void run();

  private:

    struct stop_writer_cmd_t {};

    struct write_entry_cmd_t {
        update_t update;
        flatbuffers::DetachedBuffer buffer;
    };

    using msg_t = std::variant<stop_writer_cmd_t, write_entry_cmd_t>;

    callback_t m_callback;
    journal_ptr m_journal;
    gto::cqueue<msg_t> m_queue;
    std::vector<ldb_entry_t> m_entries;
    std::condition_variable m_cond_not_empty{};
    std::mutex m_mutex;
    std::uint32_t m_queue_max_length;
    std::uint32_t m_queue_max_bytes;
    std::uint32_t m_flush_max_entries;
    std::uint32_t m_flush_max_bytes;
    std::uint32_t m_bytes_in_queue = 0;
    bool m_running = false;

    void process_messages();
};

} // namespace nplex
