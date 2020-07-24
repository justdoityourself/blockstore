/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <string_view>
#include <string>
#include <fstream>
#include <filesystem>
#include <bitset>
#include <atomic>
#include <thread>

#include "../mio.hpp"

#include "tdb/legacy.hpp"
#include "d8u/util.hpp"
#include "d8u/memory.hpp"
#include "d8u/transform.hpp"

namespace volstore
{
	using namespace std;
	using namespace gsl;

	template < typename TH > class Image
	{
		static uint64_t constexpr book_t = 256 * 1024 * 1024;
		tdb::LargeHashmapSafe db;
		tdb::_MapList<book_t, 0> dat;

		d8u::util::Statistics stats;

		bool running = true;
		std::thread manager_thread;

		std::string root;

	public:

		d8u::util::Statistics* Stats() { return &stats; }

		Image(string_view _root)
			: db(string(_root) + "/index.db")
			, dat(string(_root) + "/image.dat")
			, root(_root)
			, manager_thread([&]()
			{
				size_t counter = 0;
				while (running)
				{
					std::this_thread::sleep_for(std::chrono::milliseconds(1000));

					if (counter++ % 10 == 0)
					{
						db.Flush();
						dat.Flush();
					}

					/*
						Todo Flatten
					*/
				}
			}) 
		{ 
			if (std::filesystem::exists(string(_root) + "/lock.db"))
				throw std::runtime_error("Image is locked, is a backup running? Did a backup fail to complete gracefully? If the second is true please delete the lock file.");

			d8u::util::empty_file(string(_root) + "/lock.db");
		}

		~Image()
		{
			running = false;
			manager_thread.join();

			std::filesystem::remove(string(root) + "/lock.db");
		}

		template <typename T> bool ValidateStandard(const T& id)
		{
			auto block = Map(id);

			return d8u::transform::validate_block<T>(block);
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
				d8u::sse_vector result(map.size());

				std::copy(map.begin(), map.end(), result.begin());

				return f(result);
			});
		}

		template <typename T> d8u::sse_vector Read(const T& id)
		{
			auto map = Map(id);

			d8u::sse_vector result(map.size());

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

			dat.Flush2(block.data(), block.size());
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

	template < typename TH >class Image2
	{
		static uint64_t constexpr book_t = 256 * 1024 * 1024;
		tdb::LargeHashmapSafe db;
		std::atomic<uint64_t> file_tail;

		d8u::util::Statistics stats;

		bool running = true;
		std::thread manager_thread;

		std::string root;
		std::string image;

		std::ofstream wfile;
		std::mutex wio;

	public:

		d8u::util::Statistics* Stats() { return &stats; }

		Image2(string_view _root)
			: db(string(_root) + "/index.db")
			, image(string(_root) + "/image.dat")
			, wfile(string(_root) + "/image.dat", ios::binary | ios::app)
			, root(_root)
			, file_tail(0)
			, manager_thread([&]()
			{
				size_t counter = 0;
				while (running)
				{
					std::this_thread::sleep_for(std::chrono::milliseconds(1000));

					if (counter++ % 10 == 0)
						db.Flush();

					/*
						Todo Flatten
					*/
				}
			}) 
		{ 
			if(std::filesystem::exists(root + "/image.dat"))
				file_tail = d8u::util::GetFileSize(root + "/image.dat");

			if (std::filesystem::exists(string(_root) + "/lock.db"))
				throw std::runtime_error("Image is locked, is a backup running? Did a backup fail to complete gracefully? If the second is true please delete the lock file.");

			d8u::util::empty_file(string(_root) + "/lock.db");
		}

		~Image2()
		{
			running = false;
			manager_thread.join();

			std::filesystem::remove(string(root) + "/lock.db");
		}

		template <typename T> bool ValidateStandard(const T& id)
		{
			auto block = Read(id);

			return d8u::transform::validate_block<T>(block);
		}

		template <typename T, typename V> bool Validate(const T& id, V v)
		{
			auto block = Read(id);

			return v(block);
		}

		template <typename T> d8u::sse_vector Read(const T& id)
		{
			auto addr = db.FindLock(*((tdb::Key32*) id.data()));

			d8u::sse_vector result;

			if (!addr) return d8u::sse_vector();

			std::ifstream file(image,ios::binary);
			file.seekg(*addr, file.beg);

			uint32_t size=-1;

			file.read((char*)&size, 4);

			if (size > 1024 * 1024 * 32)
				throw std::runtime_error("Bad block size");

			result.resize(size);
			file.read((char*)result.data(), size);

			stats.atomic.items++;
			stats.atomic.read += size;

			return result;
		}

		void _Write2() {} //No Op

		template <typename T, typename Y> void _Write1(const T& id, const Y& payload)
		{
			Write(id, payload); //Passthrough
		}

		template <typename T, typename Y> void Write(const T& id, const Y& payload)
		{
			uint32_t size = (uint32_t)payload.size();

			stats.atomic.blocks++;
			stats.atomic.write += size;

			auto res = db.InsertLock(*((tdb::Key32*) id.data()), uint64_t(0));

			if (res.second && *res.first != 0)
				return; //Block has already been written.

			uint64_t o;

			{
				std::lock_guard<std::mutex> lck(wio);
				o = file_tail += (payload.size() + sizeof(uint32_t));
				o -= size + sizeof(uint32_t);

				//wfile.seekp(o); not needed with append mode;
				wfile.write((char*)&size, sizeof(uint32_t));
				wfile.write((char*)payload.data(), payload.size());
			}

			*res.first = o;
		}

		template < typename T > int _IsLocal(const T& id)
		{
			stats.atomic.queries++;

			auto* i = db.FindLock(*((tdb::Key32*) id.data()));

			return (i != nullptr && *i) ? 1 : -1;
		}

		template <size_t U, typename T> void _Many1(const T& ids) {} //No Op

		uint64_t _Many2()
		{
			return 0; //No Op
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