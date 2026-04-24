#pragma once

#include "core/types.hpp"
#include "core/robin_hood_pg_adapter.hpp"

namespace pg_volvec
{

class PgMemoryContextObject
{
public:
	static void *operator new(std::size_t size)
	{
		return MemoryContextAlloc(CurrentMemoryContext, size);
	}

	static void operator delete(void *ptr) noexcept
	{
		if (ptr != nullptr)
			pfree(ptr);
	}

	static void operator delete(void *ptr, std::size_t) noexcept
	{
		if (ptr != nullptr)
			pfree(ptr);
	}
};

template <typename T>
class PgMemoryContextAllocator
{
public:
	using value_type = T;

	PgMemoryContextAllocator() noexcept : context_(CurrentMemoryContext) {}
	explicit PgMemoryContextAllocator(MemoryContext context) noexcept
		: context_(context != nullptr ? context : CurrentMemoryContext) {}

	template <typename U>
	PgMemoryContextAllocator(const PgMemoryContextAllocator<U> &other) noexcept
		: context_(other.context()) {}

	T *allocate(std::size_t n)
	{
		if (n > (std::numeric_limits<std::size_t>::max() / sizeof(T)))
			elog(ERROR, "pg_volvec allocator size overflow");
		return static_cast<T *>(MemoryContextAlloc(context_, n * sizeof(T)));
	}

	void deallocate(T *ptr, std::size_t) noexcept
	{
		if (ptr != nullptr)
			pfree(ptr);
	}

	MemoryContext context() const noexcept { return context_; }

	template <typename U>
	bool operator==(const PgMemoryContextAllocator<U> &other) const noexcept
	{
		return context_ == other.context();
	}

	template <typename U>
	bool operator!=(const PgMemoryContextAllocator<U> &other) const noexcept
	{
		return !(*this == other);
	}

private:
	template <typename>
	friend class PgMemoryContextAllocator;

	MemoryContext context_;
};

template <typename T>
using VolVecVector = std::vector<T, PgMemoryContextAllocator<T>>;

template <typename Key, typename Value, typename Hash = std::hash<Key>>
using VolVecHashMap = RobinHoodPgMap<Key, Value, Hash>;

/* === Parallel Hash Join: Radix Partition + Linear Probe + Bloom Filter === */
static constexpr int VOLVEC_RADIX_BITS = 8;
static constexpr int VOLVEC_RADIX_FANOUT = (1 << VOLVEC_RADIX_BITS);  /* 256 partitions */
static constexpr double VOLVEC_HT_LOAD_FACTOR = 0.75;

} // namespace pg_volvec

