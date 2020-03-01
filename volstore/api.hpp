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

			void Shutdown()
			{
				http.Shutdown();
				binary.Shutdown();
			}

			~StorageService()
			{
				Shutdown();
			}

			StorageService(std::string_view path, size_t threads = 1, std::string_view http_port = "8008"
				, std::string_view is_port = "9009", std::string_view read_port = "1010", std::string_view write_port = "1111")
				: store(path)
				, http(store,http_port, threads)
				, binary(store,is_port,read_port,write_port,threads)
			{ }

			void Join()
			{
				http.Join();
				binary.Join();
			}
		};
	}
}