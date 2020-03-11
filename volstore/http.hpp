/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#define CPPHTTPLIB_KEEPALIVE_TIMEOUT_SECOND 30
#define CPPHTTPLIB_KEEPALIVE_MAX_COUNT 30

#include "../httplib.h" //This httplib library is just bad. Really bad. Why?

#include "mhttp/http.hpp"
#include "mhttp/client.hpp"
#include "d8u/string_switch.hpp"

#include "../gsl-lite.hpp"

#include <string_view>
#include <array>
#include <future>
#include <bitset>

#include "d8u/util.hpp"

namespace volstore
{
    using namespace d8u::util;
    using namespace std;
    using namespace gsl;
    using namespace httplib;

    using namespace mhttp;
    using namespace d8u;

    template <typename STORE, size_t U = 32, size_t M = 1024 * 1024> class HttpStore
    {
        HttpServer server;
        STORE &store;
    public:

        void Join()
        {
            server.Join();
        }

        void Shutdown()
        {
            server.Shutdown();
        }

        ~HttpStore()
        {
            Shutdown();
        }

        HttpStore(STORE& _store , string_view port = "8083", size_t threads = 1)
            : store(_store)
            , server( (uint16_t)stoi(port.data()),      
                [&](auto& c, auto&& req, auto body)
                {
                    switch (switch_t(req.type))
                    {
                    default:
                        c.Http400();
                        break;
                    case switch_t("GET"):
                    case switch_t("Get"):
                    case switch_t("get"):

                        switch (switch_t(req.path))
                        {
                        default:
                            c.Http400();
                            break;
                        case switch_t("/is"):
                            if (req.parameters.size() != 1)
                                return c.Http400();

                            return (store.Is(to_bin(req.parameters.begin()->second))) ? c.Http200() : c.Http404();

                        case switch_t("/many"):
                        {
                            std::vector<uint8_t> bin;

                            for (auto& e : req.parameters)
                            {
                                auto v = to_bin(e.second);
                                bin.insert(bin.end(), v.begin(), v.end());
                            }

                            std::bitset<64> bitmap(store.Many<U>(bin));
                            auto bitmap_string = bitmap.to_string();

                            std::reverse(bitmap_string.begin(), bitmap_string.end());
                            bitmap_string.resize(req.parameters.size());

                            return c.Response("200 OK", bitmap_string, std::string_view("Content-Type: text/plain\r\n"));
                        }
                        case switch_t("/read"):
                        {
                            if (req.parameters.size() != 1)
                                return c.Http400();

                            return c.Response("200 OK", store.Read(to_bin(req.parameters.begin()->second)), std::string_view("Content-Type: application/octet-stream\r\n"));
                        }
                        }

                        break;
                    case switch_t("POST"):
                    case switch_t("Post"):
                    case switch_t("post"):

                        switch (switch_t(req.path))
                        {
                        default:
                            c.Http400();
                            break;
                        case switch_t("/write"):
                        {
                            if (req.parameters.size() != 1)
                                return c.Http400();

                            store.Write(to_bin(req.parameters.begin()->second), req.body);

                            return c.Http200();
                        }
                        }
                    }

                }, false, { threads } )
        {
            //Todo use mapping
            //todo use multiplexing


        }
    };

    class HttpStoreClient
    {
        EventHttp client;
    public:
        HttpStoreClient(string_view addr = "127.0.0.1:8083") 
            : client(addr) { }

        template <typename T> std::vector<uint8_t> Read(const T& id)
        {
            std::vector<uint8_t> result;

            auto res = client.GetWait(string("/read?id=") + to_hex(id));

            result.resize(res.body.size());
            std::copy(res.body.begin(), res.body.end(), result.begin());

            return result;
        }

        template <typename T, typename Y> void Write(const T& id, const Y& payload)
        {
            auto res = client.PostWait(string("/write?id=") + to_hex(id), payload, std::string_view("Content-Type: application/octet-stream\r\n"));
        }

        template <typename T> bool Is(const T& id)
        {
            auto res = client.GetWait(string("/is?id=") + to_hex(id));

            return res.status == 200;
        }

        //Store Side validation doesn't require the block to be moved: todo
        //

        /*template <typename T, typename V> bool Validate(const T& id, V v) const
        {
            //return v(Read(id));
        }*/

        template <size_t U, typename T> uint64_t Many(const T& ids)
        {
            auto limit = ids.size() / U;

            if (limit > 64)
                throw runtime_error("The max limit for Many is 64");

            string q = "/many";

            for (size_t i = 0; i < limit; i++)
                q += string((i) ? "&" : "?") + to_string(i) + '=' + to_hex(span<uint8_t>(ids.data() + i * U, U));

            auto res = client.GetWait(q);

            std::bitset<64> result((res.body.size()) ? std::string((char*)res.body.data(), res.body.size()) : "");

            return result.to_ullong();
        }
    };

    class HttpStoreEventClient
    {
        EventHttp client;
    public:
        HttpStoreEventClient(string_view addr = "127.0.0.1:8008")
            : client(addr) { }

        void Flush()
        {
            client.Flush();
        }

        template <typename T, typename F> void Read(const T& id, F f)
        {
            client.GetCallback([f = std::move(f)](auto res)
            {
                std::vector<uint8_t> result;

                result.resize(res.body.size());
                std::copy(res.body.begin(), res.body.end(), result.begin());

                f(std::move(result));
            },string("/read?id=") + to_hex(id));
        }

        template <typename T, typename Y, typename F> void Write(const T& id, const Y& payload, F f)
        {
            client.PostCallback([f = std::move(f)](auto res)
            {
                f();
            },string("/write?id=") + to_hex(id), payload, std::string_view("Content-Type: application/octet-stream\r\n"));
        }

        template <typename T, typename F> void Is(const T& id, F f)
        {
            client.GetCallback([f = std::move(f)](auto res)
            {
                f(res.status == 200);
            },string("/is?id=") + to_hex(id));
        }

        //Store Side validation doesn't require the block to be moved: todo
        //

        /*template <typename T, typename V> bool Validate(const T& id, V v) const
        {
            //return v(Read(id));
        }*/

        template <size_t U, typename T, typename F> void Many(const T& ids, F f)
        {
            auto limit = ids.size() / U;

            if (limit > 64)
                throw runtime_error("The max limit for Many is 64");

            string q = "/many";

            for (size_t i = 0; i < limit; i++)
                q += string((i) ? "&" : "?") + to_string(i) + '=' + to_hex(span<uint8_t>(ids.data() + i * U, U));

            client.GetCallback([f = std::move(f)](auto res)
            {
                std::bitset<64> result((res.body.size()) ? std::string((char*)res.body.data(), res.body.size()) : "");

                f(result.to_ullong());
            }, q);
        }
    };

    class SimpleHttpStoreClient
    {
        string addr;
    public:
        SimpleHttpStoreClient(string_view _addr = "127.0.0.1:8083") : addr(_addr) { }

        template <typename T> std::vector<uint8_t> Read(const T& id) const
        {
            //Todo use mapping

            std::vector<uint8_t> result;
            HttpConnection client(addr);

            auto res = client.Get(string("/read?id=") + to_hex(id));

            result.resize(res.body.size());
            std::copy(res.body.begin(), res.body.end(), result.begin());

            return result;
        }

        template <typename T, typename Y> void Write(const T& id, const Y& payload) const
        {
            //todo use mapping
            HttpConnection client(addr);

            auto res = client.Post(string("/write?id=") + to_hex(id), payload, std::string_view("Content-Type: application/octet-stream\r\n"));

            //todo error handling
        }

        template <typename T> bool Is(const T& id) const
        {
            HttpConnection client(addr);

            auto res = client.Get(string("/is?id=") + to_hex(id));

            return res.status == 200;
        }

        //Store Side validation doesn't require the block to be moved: todo
        //

        /*template <typename T, typename V> bool Validate(const T& id, V v) const
        {
            //return v(Read(id));
        }*/

        template <size_t U, typename T> uint64_t Many(const T& ids) const
        {
            auto limit = ids.size() / U;

            if (limit > 64)
                throw runtime_error("The max limit for Many is 64");

            string q = "/many";

            for (size_t i = 0; i < limit; i++)
                q += string((i) ? "&" : "?") + to_string(i) + '=' + to_hex(span<uint8_t>(ids.data() + i * U, U));

            HttpConnection client(addr);

            auto res = client.Get(q);

            std::bitset<64> result((res.body.size()) ? std::string((char*)res.body.data(),res.body.size()) : "");

            return result.to_ullong();
        }
    };

	template <typename STORE, size_t U = 32, size_t M = 1024 * 1024> class _DeprecatedHTTPServer
	{
        std::thread listener;

		Server net;
        STORE store;
	public:
        ~_DeprecatedHTTPServer()
        {
            net.stop();
            listener.join();
        }

        _DeprecatedHTTPServer(string_view path, string_view host = "0.0.0.0", unsigned short port = 8083)
            : store(path)
		{
            net.Get("/is", [&](const Request& req, Response& res)
            {
                if(req.params.size()!=1)
                    return res.status = 400;

                return res.status = (store.Is(to_bin(req.params.begin()->second)) ) ? 200 : 404;
            });

            net.Get("/many", [&](const Request& req, Response& res)
            {
                std::vector<uint8_t> bin;

                for (auto& e : req.params)
                {
                    auto v = to_bin(e.second);
                    bin.insert(bin.end(), v.begin(), v.end());
                }
                    
                std::bitset<64> bitmap(store.Many<U>(bin));
                auto bitmap_string = bitmap.to_string();
           
                std::reverse(bitmap_string.begin(), bitmap_string.end());
                bitmap_string.resize(req.params.size());

                res.set_content(bitmap_string, "text/plain");

                return 200;
            });

            net.Post("/write",[&](const Request& req, Response& res, const ContentReader& content_reader)
            {
                if (req.params.size() != 1)
                    return res.status = 400;

                std::vector<uint8_t> payload; payload.reserve(0);//todo

                content_reader([&](const char* data, size_t data_length) 
                {
                    payload.insert(payload.end(),data, data+data_length);
                    return true;
                });

                store.Write(to_bin(req.params.begin()->second), payload);

                return res.status = 200;
            });

            net.Get("/read", [&](const Request& req, Response& res)
            {
                if (req.params.size() != 1)
                    return res.status = 400;

                auto v = store.Read(to_bin(req.params.begin()->second));

                res.set_content((const char*)v.data(),v.size(), "application/octet-stream");

                return 200;
            });

            net.set_error_handler([](const Request& /*req*/, Response& res) 
            {
                res.set_content("<p>Something Went Wrong 999</p>", "text/html");
            });

            net.set_logger([](const Request& req, const Response& res) { });

            listener = std::thread([&]()
            { 
                return net.listen(host.data(), port);
            });
		}
	};

    class _DeprecatedHTTPClient
    {
        int port;
        string host;
    public:
        _DeprecatedHTTPClient(string_view _host = "localhost", int _port = 8083) : host(_host) , port(_port) { }

        template <typename T> std::vector<uint8_t> Read(const T& id) const
        {
            std::vector<uint8_t> result;
            Client client(host, port);

            auto res = client.Get((string("/read?id=") + to_hex(id)).c_str(), [&](const char* data, uint64_t data_length) 
            {
                result.insert(result.end(),data, data+data_length);
                return true;
            });

            return result;
        }

        template <typename T, typename Y> void Write(const T& id, const Y& payload) const
        {
            Client client(host, port);

            auto res = client.Post((string("/write?id=") + to_hex(id)).c_str(),payload.size(), [&](uint64_t offset, uint64_t length, DataSink& sink)
            {
                sink.write((const char*)payload.data() + offset, length);
            }, "application/octet-stream");
        }

        template <typename T> bool Is(const T& id) const
        {
            Client client(host, port);

            auto res = client.Get((string("/is?id=") + to_hex(id)).c_str());

            return (res) ? ( res->status == 200 ) : false;
        }

        //Store Side validation doesn't require the block to be moved: todo
        //

        /*template <typename T, typename V> bool Validate(const T& id, V v) const
        {
            //return v(Read(id));
        }*/

        template <size_t U, typename T> uint64_t Many(const T& ids) const
        {
            auto limit = ids.size() / U;

            if (limit > 64)
                throw runtime_error("The max limit for Many is 64");

            string q = "/many";

            for (size_t i = 0; i < limit; i++)
                q += string((i) ? "&" : "?") + to_string(i) + '=' + to_hex(span<uint8_t>(ids.data() + i * U, U));

            Client client(host, port);

            auto res = client.Get(q.c_str());

            std::bitset<64> result((res) ? res->body : "");

            return result.to_ullong();
        }
    };
}