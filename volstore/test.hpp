/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <algorithm>
#include <atomic>
#include <execution>
#include <bitset>

#include "../catch.hpp"

#include "http.hpp"
#include "simple.hpp"
#include "image.hpp"
#include "api.hpp"

#include "d8u/util.hpp"

using namespace volstore;
using namespace httplib;
using namespace d8u::util;
using namespace api;

TEST_CASE("API Test", "[volstore::]")
{
    constexpr auto lim = 100;

    std::filesystem::remove_all("testimage");
    filesystem::create_directories("testimage");

    {
        StorageService svc("testimage");

        HttpStoreEventClient img;
        BinaryStoreEventClient bin;

        auto& bk = singleton<std::array<tdb::RandomKeyT<tdb::Key32>, lim>>(); // Heap

        size_t inc = 0;
        std::for_each(std::execution::seq, bk.begin(), bk.end(), [&](auto k)
        {
            if (inc++ % 2)
                img.Write(k, k, []() {});
            else
                bin.Write(k, k, []() {});
        });

        img.Flush();
        bin.Flush();

        std::atomic<size_t> finds = 0;

        inc = 0;
        std::for_each(std::execution::seq, bk.begin(), bk.end(), [&](auto k)
        {
            if (inc++ % 2)
                img.Is(k, [&](bool res)
                {
                    if(res)
                        finds++;
                });  
            else
                bin.Is(k, [&](bool res)
                {
                    if (res)
                        finds++;
                });
        });

        img.Flush();
        bin.Flush();

        CHECK(lim == finds.load());

        std::atomic<size_t> reads = 0;

        inc = 0;
        std::for_each(std::execution::seq, bk.begin(), bk.end(), [&](auto k)
        {
            if (inc++ % 2)
                img.Read(k, [&reads,k](auto res)
                {
                    if (std::equal(res.begin(), res.end(), (uint8_t*)&k))
                        reads++;
                });
            else
                bin.Read(k, [&reads, k](auto res)
                {
                    if (std::equal(res.begin(), res.end(), (uint8_t*)&k))
                        reads++;
                });
        });

        img.Flush();
        bin.Flush();

        CHECK(lim == reads.load());

        svc.Shutdown();
    }

    std::filesystem::remove_all("testimage");
}

TEST_CASE("Simple Image 1000 Blocks", "[volstore::]")
{
    constexpr auto lim = 1000;

    std::filesystem::remove_all("testimage");
    filesystem::create_directories("testimage");

    {
        Image img("testimage");

        auto& bk = singleton<std::array<tdb::RandomKeyT<tdb::Key32>, lim>>(); // Heap

        for (auto& k : bk)
            img.Write(k, k);

        size_t finds = 0;

        for (auto& k : bk)
            if (img.Is(k)) finds++;

        CHECK(lim == finds);

        finds = 0;

        for (auto k = bk.begin(); k < bk.end(); k += 5)
        {
            auto res = img.Many<32>(span<uint8_t>((uint8_t*)&(*k), 32 * 5));
            std::bitset<64> bits(res);
            finds += bits.count();
        }

        CHECK(lim == finds);

        size_t reads = 0;

        for (auto& k : bk)
        {
            auto res = img.Read(k);

            if (std::equal(res.begin(), res.end(), (uint8_t*)&k))
                reads++;
        }

        CHECK(lim == reads);
    }

    std::filesystem::remove_all("testimage");
}

TEST_CASE("Simple HTTP 10 Blocks", "[volstore::]")
{
    constexpr auto lim = 10;

    std::filesystem::remove_all("testimage");
    filesystem::create_directories("testimage");

    {
        Image backend("testimage");
        HttpStore<Image> srv(backend);

        HttpStoreClient img;

        auto& bk = singleton<std::array<tdb::RandomKeyT<tdb::Key32>, lim>>(); // Heap

        for (auto& k : bk)
            img.Write(k, k);

        size_t finds = 0;

        for (auto& k : bk)
            if (img.Is(k)) finds++;

        CHECK(lim == finds);

        finds = 0;

        for (auto k = bk.begin(); k < bk.end(); k += 5)
        {
            auto res = img.Many<32>(span<uint8_t>((uint8_t*)&(*k), 32 * 5));
            std::bitset<64> bits(res);
            finds += bits.count();
        }

        CHECK(lim == finds);

        size_t reads = 0;

        for (auto& k : bk)
        {
            auto res = img.Read(k);

            if (std::equal(res.begin(), res.end(), (uint8_t*)&k))
                reads++;
        }

        CHECK(lim == reads);
    }

    std::filesystem::remove_all("testimage");
}

TEST_CASE("Threaded HTTP", "[volstore::]")
{
    
    constexpr auto lim = 100;

    std::filesystem::remove_all("testimage");
    filesystem::create_directories("testimage");

    {
        Image backend("testimage");
        HttpStore<Image> srv(backend);

        HttpStoreClient img;

        auto& bk = singleton<std::array<tdb::RandomKeyT<tdb::Key32>, lim>>(); // Heap

        std::atomic<size_t> finds = 0;
        std::atomic<size_t> reads = 0;

        std::for_each(std::execution::par, bk.begin(), bk.end(), [&](auto k)
        {
            img.Write(k, k);

            if (img.Is(k)) finds++;

            auto res = img.Read(k);

            if (std::equal(res.begin(), res.end(), (uint8_t*)&k)) reads++;
        });

        CHECK(lim == finds.load()); 
        CHECK(lim == reads.load()); 

        finds = 0;

        std::for_each(std::execution::par, bk.begin(), bk.end(), [&](auto k)
        {
            if (img.Is(k)) finds++;
        });

        CHECK(lim == finds.load());

        reads = 0;

        std::for_each(std::execution::par, bk.begin(), bk.end(), [&](auto k)
        {
            auto res = img.Read(k);

            if (std::equal(res.begin(), res.end(), (uint8_t*)&k)) reads++;
        });

        CHECK(lim == reads.load());
    }

    std::filesystem::remove_all("testimage");
}

TEST_CASE("threaded http store", "[volstore::]")
{
    constexpr auto lim = 1000;

    std::filesystem::remove_all("testimage");
    filesystem::create_directories("testimage");

    {
        Image backend("testimage");
        HttpStore<Image> srv(backend);

        HttpStoreClient img;

        auto& bk = singleton<std::array<tdb::RandomKeyT<tdb::Key32>, lim>>(); // Heap

        std::for_each(std::execution::par, bk.begin(), bk.end(), [&](auto k)
        {
            img.Write(k, k);
        });

        std::atomic<size_t> finds = 0;

        std::for_each(std::execution::par, bk.begin(), bk.end(), [&](auto k)
        {
            if (img.Is(k)) 
                finds++;
        });

        CHECK(lim == finds.load());

        std::atomic<size_t> reads = 0;

        std::for_each(std::execution::par, bk.begin(), bk.end(), [&](auto k)
        {
            auto res = img.Read(k);

            if (std::equal(res.begin(), res.end(), (uint8_t*)&k)) 
                reads++;
        });

        CHECK(lim == reads.load());
    }

    std::filesystem::remove_all("testimage");
}

TEST_CASE("event http store", "[volstore::]")
{
    constexpr auto lim = 1000;

    std::filesystem::remove_all("testimage");
    filesystem::create_directories("testimage");

    {
        Image backend("testimage");
        HttpStore<Image> srv(backend);

        HttpStoreEventClient img("127.0.0.1:8083");

        auto& bk = singleton<std::array<tdb::RandomKeyT<tdb::Key32>, lim>>(); // Heap

        std::for_each(std::execution::seq, bk.begin(), bk.end(), [&](auto k)
        {
            img.Write(k, k, []() {});
        });

        img.Flush();

        std::atomic<size_t> finds = 0;

        std::for_each(std::execution::seq, bk.begin(), bk.end(), [&](auto k)
        {
            img.Is(k, [&](bool res)
            {
                if(res)
                    finds++;
            });  
        });

        img.Flush();

        CHECK(lim == finds.load());

        std::atomic<size_t> reads = 0;

        std::for_each(std::execution::seq, bk.begin(), bk.end(), [&](auto k)
        {
            img.Read(k, [&reads,k](auto res)
            {
                if (std::equal(res.begin(), res.end(), (uint8_t*)&k))
                    reads++;
            });
        });

        img.Flush();

        CHECK(lim == reads.load());
    }

    std::filesystem::remove_all("testimage");
}



TEST_CASE("Threaded Image 100,000 Blocks mixed IO", "[volstore::]")
{
    constexpr auto lim = 100000;

    std::filesystem::remove_all("testimage");
    filesystem::create_directories("testimage");

    {
        Image img("testimage");

        auto& bk = singleton<std::array<tdb::RandomKeyT<tdb::Key32>, lim>>(); // Heap

        std::atomic<size_t> finds = 0;
        std::atomic<size_t> reads = 0;

        std::for_each(std::execution::par, bk.begin(), bk.end(), [&](auto k)
        {
            img.Write(k, k);

            if (img.Is(k)) finds++;

            auto res = img.Read(k);

            if (std::equal(res.begin(), res.end(), (uint8_t*)&k)) reads++;
        });

        CHECK(lim == finds.load()); 
        CHECK(lim == reads.load()); 

        finds = 0;

        std::for_each(std::execution::par, bk.begin(), bk.end(), [&](auto k)
        {
            if (img.Is(k)) finds++;
        });

        CHECK(lim == finds.load());

        reads = 0;

        std::for_each(std::execution::par, bk.begin(), bk.end(), [&](auto k)
        {
            auto res = img.Read(k);

            if (std::equal(res.begin(), res.end(), (uint8_t*)&k)) reads++;
        });

        CHECK(lim == reads.load());
    }

    std::filesystem::remove_all("testimage");
}

TEST_CASE("Threaded Image 100,000 Blocks", "[volstore::]")
{
    constexpr auto lim = 100000;

    std::filesystem::remove_all("testimage");
    filesystem::create_directories("testimage");

    {
        Image img("testimage");

        auto& bk = singleton<std::array<tdb::RandomKeyT<tdb::Key32>, lim>>(); // Heap

        std::for_each(std::execution::par, bk.begin(), bk.end(), [&](auto k)
        {
            img.Write(k, k);
        });

        std::atomic<size_t> finds = 0;

        std::for_each(std::execution::par, bk.begin(), bk.end(), [&](auto k)
        {
            if (img.Is(k)) finds++;
        });

        CHECK(lim == finds.load());

        std::atomic<size_t> reads = 0;

        std::for_each(std::execution::par, bk.begin(), bk.end(), [&](auto k)
        {
            auto res = img.Read(k);

            if (std::equal(res.begin(), res.end(), (uint8_t*)&k)) reads++;
        });

        CHECK(lim == reads.load());
    }

    std::filesystem::remove_all("testimage");
}



