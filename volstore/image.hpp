/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <string_view>
#include <string>
#include <fstream>
#include <filesystem>
#include <bitset>
#include <atomic>

#include "../mio.hpp"

#include "tdb/database.hpp"

namespace volstore
{
	using namespace std;
	using namespace gsl;

	/*
		Finishing the database layer left this version obsolete.
	*/

	class _OldImageDeprecated
	{
		std::string index_file;
		std::string data_file;

		tdb::SafeWriter<tdb::MediumIndex> wdb;
		tdb::ViewManager<tdb::MediumIndexReadOnly> rdb;
		tdb::ViewManager<tdb::_MapFile<128 * 1024 * 1024>> mm;

	public:

		_OldImageDeprecated(string_view _root)
			: index_file(string(_root) + "/index.db")
			, data_file(string(_root) + "/image.dat")
			, rdb(index_file)
			, wdb(index_file)
			, mm(data_file) { }

		template <typename T> std::vector<uint8_t> Read(const T& id)
		{
			auto addr = rdb.View().Map().Find( *( (tdb::Key32*)id.data() ) );

			if (!addr) return std::vector<uint8_t>();

			auto view = mm.Fixed(*addr);
			auto block = view.Map().data() + *addr;

			auto size = *( (uint32_t*)block);
			std::vector<uint8_t> result(size);

			std::copy( block+sizeof(uint32_t), block + sizeof(uint32_t)+size,result.begin());

			return result;
		}

		template <typename T, typename Y> void Write(const T& id, const Y& payload)
		{
			auto alloc = mm.Alloc(sizeof(uint32_t) + payload.size());
			auto offset = alloc.first.second;
			auto dest = alloc.first.first;

			auto res = wdb.GetLock().GetWriter().Insert(*((tdb::Key32*)id.data()), offset);
	
			*((uint32_t*)dest) = (uint32_t)payload.size();
			std::copy(payload.begin(), payload.end(), dest + sizeof(uint32_t));
		}

		template <typename T> bool Is(const T& id)
		{
			return rdb.View().Map().Find(*((tdb::Key32*)id.data())) != nullptr;
		}

		template <size_t U, typename T> uint64_t Many(const T& ids)
		{
			std::bitset<64> result;

			auto limit = ids.size() / U;

			if (limit > 64)
				throw runtime_error("The max limit for Many is 64");

			auto view = rdb.View();

			for (size_t i = 0; i < limit; i++)
				result.set(i, view.Map().Find(*(((tdb::Key32*)ids.data())+i)) != nullptr);

			return result.to_ullong();
		}
	};

	class Image
	{
		tdb::LargeHashmapSafe db;
		tdb::_MapList<256 * 1024 * 1024, 0> dat;

	public:

		Image(string_view _root)
			: db(string(_root) + "/index.db")
			, dat(string(_root) + "/image.dat") { }

		template <typename T> gsl::span<uint8_t> Map(const T& id)
		{
			auto addr = db.FindLock(*((tdb::Key32*) id.data()));

			if (!addr) return gsl::span<uint8_t>();

			auto block = dat.offset(*addr);

			if (!block) return gsl::span<uint8_t>();

			auto size = *((uint32_t*)block);

			return gsl::span<uint8_t>(block + sizeof(uint32_t), size);
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
			auto res = db.InsertLock( *( (tdb::Key32*) id.data() ), uint64_t(0));

			//Duplicate block insert?
			//

			if (res.second && *res.first != 0) return gsl::span<uint8_t>();

			auto [p, o] = dat.AllocateAlign(size + sizeof(uint32_t));

			if(!p) return gsl::span<uint8_t>();

			*((uint32_t*)p) = (uint32_t)size;

			*res.first = o;

			return gsl::span<uint8_t>(p+sizeof(uint32_t),size);
		}

		template <typename T, typename Y> void Write(const T& id, const Y& payload)
		{
			auto block = Allocate(id, payload.size());

			std::copy(payload.begin(), payload.end(), block.begin());
		}

		template <typename T> bool Is(const T& id)
		{
			return db.FindLock( *( (tdb::Key32*) id.data() ) ) != nullptr;
		}

		template <size_t U, typename T> uint64_t Many(const T& ids)
		{
			std::bitset<64> result;

			auto limit = ids.size() / U;

			if (limit > 64)
				throw runtime_error("The max limit for Many is 64");

			for (size_t i = 0; i < limit; i++)
				result.set(i, db.FindLock( *( ( (tdb::Key32*)ids.data() ) + i) ) != nullptr);

			return result.to_ullong();
		}
	};
}