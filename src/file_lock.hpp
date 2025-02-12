#include <sys/file.h>
#include <unistd.h>
#include <stdexcept>
#include <string>
#include <filesystem>
#include <fmt/format.h>

namespace nplex {

/**
 * RAII class for managing a file lock.
 * 
 * This class ensures that a lock file is created, locked, and removed
 * properly using the RAII idiom. It prevents multiple instances of the
 * program from running simultaneously by locking the specified file.
 */
class file_lock_t
{
  public:

    file_lock_t() = default;

    /**
     * Constructs a file_lock_t and locks the specified file.
     * 
     * @param[in] lock_file The path to the lock file.
     * @throws std::runtime_error if the lock file cannot be created or locked.
     */
    file_lock_t(const std::filesystem::path &lock_file) : m_lock_file(lock_file)
    {
        m_fd = open(m_lock_file.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0660);
        if (m_fd == -1)
            throw std::runtime_error(fmt::format("Error: Unable to create lock file {}", m_lock_file.string()));

        if (flock(m_fd, LOCK_EX | LOCK_NB) == -1) {
            close(m_fd);
            throw std::runtime_error("Error: Another instance of the program is already running");
        }

        std::string pid_str = std::to_string(getpid());
        if (write(m_fd, pid_str.c_str(), pid_str.size()) == -1) {
            close(m_fd);
            std::filesystem::remove(m_lock_file);
            throw std::runtime_error(fmt::format("Error: Unable to write to lock file {}", m_lock_file.string()));
        }
    }

    /**
     * Destructor that releases the lock and removes the lock file.
     */
    ~file_lock_t()
    {
        if (m_fd != -1) {
            close(m_fd);
            std::filesystem::remove(m_lock_file);
        }
    }

    friend void swap(file_lock_t& first, file_lock_t& second) noexcept {
        using std::swap;
        swap(first.m_lock_file, second.m_lock_file);
        swap(first.m_fd, second.m_fd);
    }

    file_lock_t(file_lock_t&& other) noexcept : file_lock_t() {
        swap(*this, other);
    }

    file_lock_t & operator=(file_lock_t&& other) noexcept {
        swap(*this, other);
        return *this;
    }

    // Copy is not allowed
    file_lock_t(const file_lock_t&) = delete;
    file_lock_t& operator=(const file_lock_t&) = delete;

  private:
    std::filesystem::path m_lock_file;
    int m_fd = -1;
};

} // namespace nplex
