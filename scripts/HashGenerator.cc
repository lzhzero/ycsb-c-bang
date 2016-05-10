/*
 * Author: Ning Gao(nigo9731@colorado.edu)
 */

#include <iostream>
#include <cstdint>
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

int main(void){
	for(int i=0; i< 10000000; ++i){
		std::cout<<"user"<<Hash(i)<<std::endl;
	}
	return 0;
}
