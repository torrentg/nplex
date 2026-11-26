#pragma once

#include <atomic>
#include <cstdint>
#include <utility>

/**
 * @file counted_ptr.hpp
 * @brief A lightweight single-allocation reference-counted smart pointer for C++.
 * @see https://github.com/torrentg/counted_ptr
 * @version 0.8.0
 */

namespace gto {

/**
* Internal storage used by counted_ptr.
*
* This type is an implementation detail and is not intended to be
* used directly by library users. It contains the reference counter
* and the managed object in a single allocation.
*
* @tparam T Type of the managed object.
*/
template<typename T>
struct counted_storage_t final
{
    template<typename... Args>
    explicit counted_storage_t(Args&&... args) : obj(std::forward<Args>(args)...) {}
    counted_storage_t(const counted_storage_t &) = delete;
    counted_storage_t &operator=(const counted_storage_t &) = delete;

    std::atomic<std::uint32_t> ref_count{1};
    T obj;
};

/**
 * Reference-counted smart pointer.
 *
 * This class provides shared ownership of an object through a pointer.
 * The managed object is destroyed when the last counted_ptr owning it is released.
 * 
 * Unlike std::shared_ptr:
 * - Uses a single pointer representation.
 * - Does not support weak references.
 * - Does not support custom deleters.
 * - Creating a new managed object requires make_counted().
 * - Construction from raw pointers is intentionally not supported.
 *
 * @tparam T Type of the managed object.
 */
template<typename T>
class counted_ptr
{
    static_assert(!std::is_reference_v<T>, "counted_ptr does not support reference types.");

  public:

    counted_ptr() noexcept = default;
    counted_ptr(std::nullptr_t) noexcept {}

    counted_ptr(const counted_ptr &other) noexcept : m_storage(other.m_storage) {
        if (m_storage)
            m_storage->ref_count.fetch_add(1, std::memory_order_relaxed);
    }

    counted_ptr(counted_ptr &&other) noexcept : m_storage(std::exchange(other.m_storage, nullptr)) {}

    ~counted_ptr() {
        if (m_storage && m_storage->ref_count.fetch_sub(1, std::memory_order_acq_rel) == 1)
            delete m_storage;
    }

    counted_ptr& operator=(const counted_ptr &other) noexcept {
        if (this != &other) {
            counted_ptr tmp(other);
            swap(tmp);
        }
        return *this;
    }

    counted_ptr& operator=(std::nullptr_t) noexcept {
        reset();
        return *this;
    }

    counted_ptr& operator=(counted_ptr &&other) noexcept {
        if (this != &other)
            swap(other);
        return *this;
    }

    bool operator==(const counted_ptr &other) const noexcept {
        return (m_storage == other.m_storage);
    }

    bool operator!=(const counted_ptr &other) const noexcept {
        return (m_storage != other.m_storage);
    }

    bool operator==(std::nullptr_t) const noexcept {
        return (m_storage == nullptr);
    }

    bool operator!=(std::nullptr_t) const noexcept {
        return (m_storage != nullptr);
    }

    T* operator->() const noexcept {
        return &m_storage->obj;
    }

    T& operator*() const noexcept {
        return m_storage->obj;
    }

    T* get() const noexcept {
        return (m_storage ? &m_storage->obj : nullptr);
    }

    explicit operator bool() const noexcept {
        return (m_storage != nullptr);
    }

    std::uint32_t use_count() const noexcept {
        return (m_storage ? m_storage->ref_count.load(std::memory_order_relaxed) : 0);
    }

    void reset() noexcept {
        counted_ptr tmp;
        swap(tmp);
    }

    void swap(counted_ptr &other) noexcept {
        std::swap(m_storage, other.m_storage);
    }

  public: // Friend functions

    template<typename U, typename... Args>
    friend counted_ptr<U> make_counted(Args&&... args);

  private:

    counted_storage_t<T> *m_storage = nullptr;

    explicit counted_ptr(counted_storage_t<T> *storage) noexcept : m_storage(storage) {}
};

/**
 * Creates a reference-counted object.
 *
 * This is the only supported way to create a counted_ptr owning a new object.
 * Adoption of raw pointers is intentionally not supported.
 *
 * @tparam T Type of the managed object.
 * @param[in] args Arguments forwarded to T's constructor.
 * @return A counted_ptr<T> owning the created object.
 */
template<typename T, typename... Args>
[[nodiscard]] counted_ptr<T> make_counted(Args&&... args) {
    return counted_ptr<T>(new counted_storage_t<T>(std::forward<Args>(args)...));
}

/**
 * Swaps two counted_ptr instances.
 *
 * Allows using the trick:
 *     using std::swap;
 *     swap(a, b);
 * 
 * @tparam T Type of the managed objects.
 * @param[in] lhs First counted_ptr.
 * @param[in] rhs Second counted_ptr.
 */
template<typename T>
void swap(counted_ptr<T> &lhs, counted_ptr<T> &rhs) noexcept {
    lhs.swap(rhs);
}

/**
 * Compares a nullptr with counted_ptr for equality.
 *
 * Covers the case 'nullptr == my_counted_ptr'
 * 
 * @tparam T Type of the managed object.
 * @param[in] ptr The counted_ptr to compare.
 * @return True if the counted_ptr is null, false otherwise.
 */
template<typename T>
bool operator==(std::nullptr_t, const counted_ptr<T> &ptr) noexcept {
    return (ptr == nullptr);
}

/**
 * Compares a nullptr with counted_ptr for equality.
 *
 * Covers the case 'nullptr != my_counted_ptr'
 * 
 * @tparam T Type of the managed object.
 * @param[in] ptr The counted_ptr to compare.
 * @return True if the counted_ptr is not null, false otherwise.
 */
template<typename T>
bool operator!=(std::nullptr_t, const counted_ptr<T> &ptr) noexcept {
    return (ptr != nullptr);
}

} // namespace gto
