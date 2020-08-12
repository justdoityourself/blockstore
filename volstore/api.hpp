/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include "http.hpp"
#include "binary.hpp"
#include "image.hpp"

#include "kreg/service.hpp"

#include <string_view>

namespace volstore
{
	namespace api
	{
		template < typename TH > class StorageService
		{
			Image<TH> store;
			HttpStore<Image<TH>> http;
			BinaryStore<Image<TH>> binary;
			kreg::Service registry;

		public:

			d8u::util::Statistics* Stats() { return store.Stats(); }

			size_t ConnectionCount() { return http.ConnectionCount() + binary.ConnectionCount(); }
			size_t MessageCount() { return http.MessageCount() + binary.MessageCount(); }
			size_t EventsStarted() { return http.EventsStarted() + binary.EventsStarted(); }
			size_t EventsFinished() { return http.EventsFinished() + binary.EventsFinished(); }
			size_t ReplyCount() { return http.ReplyCount() + binary.ReplyCount(); }

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
				, std::string_view is_port = "9009", std::string_view read_port = "1010", std::string_view write_port = "1111", std::string_view registry_port = "7007",bool print = true)
				: store(path)
				, http(store,http_port, threads)
				, binary(store,is_port,read_port,write_port,threads, buffered_writes)
				, registry(registry_port,std::string(path) + "/registry.db")
			{ 
				if (print)
				{
					std::cout << "HTTP: " << http_port << std::endl;
					std::cout << "QUERY: " << is_port << std::endl;
					std::cout << "READ: " << read_port << std::endl;
					std::cout << "WRITE: " << write_port << std::endl;
					std::cout << "REGISTRY: " << registry_port << std::endl;
				}
			}

			void Join()
			{
				http.Join();
				binary.Join();
			}
		};

		template < typename TH > class StorageService2
		{
			Image2<TH> store;
			HttpStore<Image2<TH>> http;
			BinaryStore2<Image2<TH>> binary;
			kreg::Service registry;

		public:

			d8u::util::Statistics* Stats() { return store.Stats(); }

			size_t ConnectionCount() { return http.ConnectionCount() + binary.ConnectionCount(); }
			size_t MessageCount() { return http.MessageCount() + binary.MessageCount(); }
			size_t EventsStarted() { return http.EventsStarted() + binary.EventsStarted(); }
			size_t EventsFinished() { return http.EventsFinished() + binary.EventsFinished(); }
			size_t ReplyCount() { return http.ReplyCount() + binary.ReplyCount(); }

			void Shutdown()
			{
				http.Shutdown();
				binary.Shutdown();
			}

			~StorageService2()
			{
				Shutdown();
			}

			StorageService2(std::string_view path, int start_code, size_t threads = 1, std::string_view http_port = "8008"
				, std::string_view is_port = "9009", std::string_view read_port = "1010", std::string_view write_port = "1111", std::string_view registry_port = "7007", bool print = true)
				: store(path, start_code)
				, http(store, http_port, threads)
				, binary(store, is_port, read_port, write_port, threads)
				, registry(registry_port, std::string(path) + "/registry.db")
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