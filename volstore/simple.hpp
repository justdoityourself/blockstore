/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <vector>
#include <string_view>
#include <string>
#include <fstream>
#include <filesystem>
#include <bitset>

#include "../mio.hpp"
#include "../gsl-lite.hpp"

#include "tdb/mapping.hpp"
#include "d8u/util.hpp"
#include "d8u/string.hpp"

namespace volstore
{
	using namespace std;
	using namespace gsl;
	using namespace d8u::util;

	class Simple
	{
		string root;
	public:
		Simple(string_view _root) : root(_root) { }

		template <typename T> mio::mmap_source Map(const T& id) const
		{
			return mio::mmap_source(root + "/" + to_hex(id));
		}

		template <typename T> std::vector<uint8_t> Read(const T & id) const
		{
			auto file = Map(id);

			std::vector<uint8_t> result(file.size());
			std::copy(file.begin(), file.end(), result.begin());

			return result;
		}

		template <typename T, typename Y> mio::mmap_sink Allocate(const T& id, size_t size) const
		{
			auto file = root + "/" + to_hex(id);

			tdb::empty_file1(file);
			std::filesystem::resize_file(file, size);
			
			return mio::mmap_sink(file);
		}

		template <typename T, typename Y> void Write(const T& id, const Y& payload) const
		{
			ofstream(root + "/" + to_hex(id), ofstream::binary).write((char*)payload.data(), payload.size());
		}

		template <typename T> bool Is(const T& id) const
		{
			return filesystem::exists(root + "/" + to_hex(id));
		}

		//Store Side validation doesn't require the block to be moved:
		//

		template <typename T, typename V> bool Validate(const T& id, V v) const
		{
			return v(Read(id));
		}

		template <size_t U,typename T> uint64_t Many(const T& ids) const
		{
			std::bitset<64> result;

			auto limit = ids.size() / U;

			if (limit > 64)
				throw runtime_error("The max limit for Many is 64");

			for (size_t i = 0; i < limit; i++)
				result.set(i, filesystem::exists(root + "/" + to_hex(span<uint8_t>((uint8_t*)ids.data()+U*i,U))));

			return result.to_ullong();
		}
	};
}