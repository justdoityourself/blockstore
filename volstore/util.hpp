/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <string>

namespace volstore
{
	namespace util
	{
        using namespace std;

        constexpr char hexmap[] = { '0', '1', '2', '3', '4', '5', '6', '7',
                                    '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

        template <typename T> string to_hex(const T & d)
        {
            string s(d.size() * 2, ' ');
            for (size_t i = 0; i < d.size(); ++i) 
            {
                s[2 * i] =      hexmap[(d[i] & 0xF0) >> 4];
                s[2 * i + 1] =  hexmap[d[i] & 0x0F];
            }
            return s;
        }

		vector<uint8_t> to_bin(std::string_view v)
		{
			std::vector<uint8_t> result; result.reserve(v.size() / 2 + 1);
			auto ctoi = [](char c)
			{
				if (c >= '0' && c <= '9')
					return c - '0';
				if (c >= 'A' && c <= 'F')
					return c - 'A' + 10;
				if (c >= 'a' && c <= 'f')
					return c - 'a' + 10;

				return 0;
			};

			for (auto c = v.begin(); c < v.end(); c += 2)
				result.push_back((ctoi(*c) << 4) + ctoi(*(c + 1)));

			return result;
		}
	}
}