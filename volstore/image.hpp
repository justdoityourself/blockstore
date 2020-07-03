/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <string_view>
#include <string>
#include <fstream>
#include <filesystem>
#include <bitset>
#include <atomic>

#include "../mio.hpp"

#include "tdb/legacy.hpp"
#include "d8u/util.hpp"
#include "d8u/transform.hpp"

namespace volstore
{
	using namespace std;
	using namespace gsl;

	class Image
	{
		static uint64_t constexpr book_t = 256 * 1024 * 1024;
		tdb::LargeHashmapSafe db;
		tdb::_MapList<book_t, 0> dat;

		d8u::util::Statistics stats;

	public:

		d8u::util::Statistics* Stats() { return &stats; }

		Image(string_view _root)
			: db(string(_root) + "/index.db")
			, dat(string(_root) + "/image.dat") { }

		template <typename T> bool ValidateStandard(const T& id)
		{
			auto block = Map(id);

			return d8u::transform::validate_block(block);
		}

		template <typename T, typename V> bool Validate(const T& id, V v)
		{
			auto block = Read(id);

			return v(block);
		}

		template <typename T> gsl::span<uint8_t> Map(const T& id)
		{
			auto addr = db.FindLock(*((tdb::Key32*) id.data()));

			if (!addr) return gsl::span<uint8_t>();

			auto block = dat.offset(*addr);

			if (!block) return gsl::span<uint8_t>();

			auto size = *((uint32_t*)block);

			stats.atomic.items++;
			stats.atomic.read += size;

			return gsl::span<uint8_t>(block + sizeof(uint32_t), size);
		}

		template <typename F> uint64_t EnumerateMap(uint64_t start, F&& f)
		{
			bool _continue = true;

			while (_continue && start < dat.size())
			{
				auto block = dat.offset(start);
				auto size = *((uint32_t*)block);

				if (!size)
				{
					//Alignment Gap: Jump to next book
					//

					start += book_t - (start % book_t);
					continue;
				}

				_continue = f(gsl::span<uint8_t>(block + sizeof(uint32_t), size));

				start += sizeof(uint32_t) + size;
			}

			return start;
		}

		template <typename F> uint64_t Enumerate(uint64_t start, F&& f)
		{
			return EnumerateMap(start, [f = std::move(f)](auto map)
			{
				std::vector<uint8_t> result(map.size());

				std::copy(map.begin(), map.end(), result.begin());

				return f(result);
			});
		}

		template <typename T> std::vector<uint8_t> Read(const T& id)
		{
			auto map = Map(id);

			std::vector<uint8_t> result(map.size());

			std::copy(map.begin(), map.end(), result.begin());

			return result;
		}

		template <typename T> gsl::span<uint8_t> Allocate(const T& id, size_t size)
		{
			stats.atomic.blocks++;
			stats.atomic.write += size;

			//Race condition:
			//A block request happening around the same time will report zero as the offset.
			//Up until the line below: "*res.first = o;"
			//This has been resolved by reporting a miss for all uninitialized pointers.

			auto res = db.InsertLock( *( (tdb::Key32*) id.data() ), uint64_t(0));

			//Duplicate block insert? Abort.
			//This happens usually when the client isn't using a local block filter.
			//

			if (res.second && *res.first != 0) 
				return gsl::span<uint8_t>();

			//This code will align blocks to the page at the cost of packing. Not needed.
			//auto [p, o] = dat.AllocateAlign(size + sizeof(uint32_t));
			//

			auto [p, o] = dat.Allocate(size + sizeof(uint32_t));

			if(!p) 
				return gsl::span<uint8_t>();

			*((uint32_t*)p) = (uint32_t)size;

			*res.first = o;

			return gsl::span<uint8_t>(p+sizeof(uint32_t),size);
		}

		template <typename T, typename Y> void Write(const T& id, const Y& payload)
		{
			auto block = Allocate(id, payload.size());

			if (!block.data())
				return; //Todo notification, however this never causes problems.

			std::copy(payload.begin(), payload.end(), block.begin());
		}

		template <typename T> bool Is(const T& id)
		{
			stats.atomic.queries++;

			auto* i = db.FindLock(*((tdb::Key32*) id.data()));

			return i != nullptr && *i;
		}

		template <size_t U, typename T> uint64_t Many(const T& ids)
		{
			std::bitset<64> result;

			auto limit = ids.size() / U;

			stats.atomic.queries += limit;

			if (limit > 64)
				throw runtime_error("The max limit for Many is 64");

			for (size_t i = 0; i < limit; i++)
			{
				auto* k = db.FindLock(*(((tdb::Key32*)ids.data()) + i));
				result.set(i, (k != nullptr && *k));
			}		

			return result.to_ullong();
		}
	};
}