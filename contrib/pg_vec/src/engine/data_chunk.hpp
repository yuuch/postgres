#ifndef PG_VEC_DATA_CHUNK_HPP
#define PG_VEC_DATA_CHUNK_HPP

#include <cstdint>

namespace pg_vec
{

template <typename T, std::uint16_t Capacity>
struct FlatVector
{
	T			values[Capacity];

	T &
	operator[](std::uint16_t row)
	{
		return values[row];
	}

	const T &
	operator[](std::uint16_t row) const
	{
		return values[row];
	}
};

template <std::uint16_t Capacity>
struct SelectionVector
{
	std::uint16_t row_ids[Capacity];
	std::uint16_t count;

	void
	clear()
	{
		count = 0;
	}

	void
	append(std::uint16_t row)
	{
		row_ids[count++] = row;
	}

	std::uint16_t
	operator[](std::uint16_t idx) const
	{
		return row_ids[idx];
	}
};

template <std::uint16_t Capacity>
struct DataChunkHeader
{
	std::uint16_t count;
	SelectionVector<Capacity> sel;

	void
	reset()
	{
		count = 0;
		sel.clear();
	}
};

} /* namespace pg_vec */

#endif
