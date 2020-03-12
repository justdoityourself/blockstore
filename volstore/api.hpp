/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include "http.hpp"
#include "binary.hpp"
#include "image.hpp"


#include <string_view>

namespace volstore
{
	namespace api
	{
		class StorageService
		{
			Image store;
			HttpStore<Image> http;
			BinaryStore<Image> binary;

		public:

			d8u::util::Statistics* Stats() { return store.Stats(); }

			size_t ConnectionCount()
			{
				return http.ConnectionCount() + binary.ConnectionCount();
			}

			void Shutdown()
			{
				http.Shutdown();
				binary.Shutdown();
			}

			~StorageService()
			{
				Shutdown();
			}

			StorageService(std::string_view path, size_t threads = 1, bool buffered_writes=true, std::string_view http_port = "8008"
				, std::string_view is_port = "9009", std::string_view read_port = "1010", std::string_view write_port = "1111",bool print = true)
				: store(path)
				, http(store,http_port, threads)
				, binary(store,is_port,read_port,write_port,threads, buffered_writes)
			{ 
				if (print)
				{
					std::cout << "HTTP: " << http_port << std::endl;
					std::cout << "QUERY: " << is_port << std::endl;
					std::cout << "READ: " << read_port << std::endl;
					std::cout << "WRITE: " << write_port << std::endl;
				}
			}

			void Join()
			{
				http.Join();
				binary.Join();
			}
		};
	}
}