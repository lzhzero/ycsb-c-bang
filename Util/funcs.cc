/*
 * funcs.cc
 *
 *  Created on: Mar 2, 2017
 *      Author: neal
 */

#include "funcs.h"
namespace Ycsb {
namespace Util {

std::unordered_map<std::pair<float, float>, RandFloatSeed*,
		PairHash<float, float>> randFloatSeeds;

} // Util
} // Ycsb
