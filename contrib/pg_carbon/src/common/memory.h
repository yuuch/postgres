#ifndef PG_CARBON_MEMORY_H
#define PG_CARBON_MEMORY_H

#include <deque>
#include <list>
#include <memory>
#include <new>
#include <stack>
#include <vector>

extern "C" {
#include "postgres.h"
#include "utils/memutils.h"
}

namespace pg_carbon {

// C++ Allocator using palloc/pfree
template <typename T> class PgAllocator {
public:
  using value_type = T;

  PgAllocator(MemoryContext ctx = nullptr) : ctx_(ctx) {}
  template <typename U>
  PgAllocator(const PgAllocator<U> &other) : ctx_(other.ctx_) {}

  T *allocate(std::size_t n) {
    if (n > std::size_t(-1) / sizeof(T))
      throw std::bad_alloc();

    void *p;
    if (ctx_) {
      p = MemoryContextAlloc(ctx_, n * sizeof(T));
    } else {
      p = palloc(n * sizeof(T));
    }

    if (p)
      return static_cast<T *>(p);
    throw std::bad_alloc();
  }

  void deallocate(T *p, std::size_t) { pfree(p); }

  MemoryContext GetContext() const { return ctx_; }

private:
  MemoryContext ctx_;
  template <typename U> friend class PgAllocator;
};

template <typename T, typename U>
bool operator==(const PgAllocator<T> &a, const PgAllocator<U> &b) {
  return a.GetContext() == b.GetContext();
}

template <typename T, typename U>
bool operator!=(const PgAllocator<T> &a, const PgAllocator<U> &b) {
  return a.GetContext() != b.GetContext();
}

// Base class for objects to be allocated in PG memory context
class PgObject {
public:
  static void *operator new(std::size_t size) { return palloc(size); }
  static void *operator new(std::size_t size, MemoryContext ctx) {
    return MemoryContextAlloc(ctx, size);
  }

  static void operator delete(void *ptr) { pfree(ptr); }
  static void operator delete(void *ptr, MemoryContext ctx) { pfree(ptr); }

  // Placement new/delete
  static void *operator new(std::size_t size, void *ptr) { return ptr; }
  static void operator delete(void *ptr, void *voidptr2) {}

  virtual ~PgObject() = default;
};

// Container aliases
template <typename T> using PgVector = std::vector<T, PgAllocator<T>>;

template <typename T> using PgList = std::list<T, PgAllocator<T>>;

template <typename T> using PgDeque = std::deque<T, PgAllocator<T>>;

template <typename T> using PgStack = std::stack<T, PgDeque<T>>;

} // namespace pg_carbon

#endif // PG_CARBON_MEMORY_H
