//
//  utils.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/5/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_UTILS_H_
#define YCSB_C_UTILS_H_

#include <cstdint>
#include <random>
#include <algorithm>
#include <unordered_map>
#include "Exception.h"
namespace Ycsb {
namespace Util {

const uint64_t kFNVOffsetBasis64 = 0xCBF29CE484222325;
const uint64_t kFNVPrime64 = 1099511628211;

inline uint64_t FNVHash64(uint64_t val) {
	uint64_t hash = kFNVOffsetBasis64;

	for (int i = 0; i < 8; i++) {
		uint64_t octet = val & 0x00ff;
		val = val >> 8;

		hash = hash ^ octet;
		hash = hash * kFNVPrime64;
	}
	return hash;
}

inline uint64_t Hash(uint64_t val) {
	return FNVHash64(val);
}

inline double RandomDouble(double min = 0.0, double max = 1.0) {
	static std::default_random_engine generator;
	static std::uniform_real_distribution<double> uniform(min, max);
	return uniform(generator);
}

template<class T1, class T2>
struct PairHash {
	std::size_t operator ()(const std::pair<T1, T2> &p) const {
		auto h1 = std::hash<T1> { }(p.first);
		auto h2 = std::hash<T2> { }(p.second);
		return h1 ^ h2;
	}
};

struct RandFloatSeed {
	std::default_random_engine generator;
	std::uniform_real_distribution<float> uniform;

	RandFloatSeed(float min, float max) :
			uniform(min, max) {
		struct timespec now;
		clock_gettime(CLOCK_MONOTONIC_RAW, &now);
		generator = std::default_random_engine(now.tv_nsec);
	}

	float Next(){
		return uniform(generator);
	}
};

///
/// Returns an ASCII code that can be printed to desplay
///
inline char RandomPrintChar() {
	return rand() % 94 + 33;
}

inline bool StrToBool(std::string str) {
	std::transform(str.begin(), str.end(), str.begin(), ::tolower);
	if (str == "true" || str == "1") {
		return true;
	} else if (str == "false" || str == "0") {
		return false;
	} else {
		throw Exception("Invalid bool string: " + str);
	}
}

inline std::string Trim(const std::string &str) {
	auto front = std::find_if_not(str.begin(), str.end(),
			[](int c) {return std::isspace(c);});
	return std::string(front,
			std::find_if_not(str.rbegin(),
					std::string::const_reverse_iterator(front),
					[](int c) {return std::isspace(c);}).base());
}

} // Util
} // Ycsb
#endif // YCSB_C_UTILS_H_
