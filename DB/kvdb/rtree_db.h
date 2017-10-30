/*
 * rtree_db.h
 *
 *  Created on: Mar 1, 2017
 *      Author: neal
 */

#ifndef YCSB_C_RTREE_DB_H_
#define YCSB_C_RTREE_DB_H_

#include "ddsbrick.h"
#include "RTree/Core/RTree.h"
#include "DB/commons.h"
#include "Util/funcs.h"

namespace Ycsb {
using std::cout;
using std::endl;
typedef RTree::Core::RTree<RTree::Core::StringDataType, float, 3, float, 30, 5> RTreeString;

bool QueryResultCallback(RTree::Core::StringDataType a_data, void* a_context) {
	return true;
}

namespace DB {
/*
 * Generate workload based on MAX_WORLDSIZE and FRAC_WORLDSIZE
 */
const float FRAC_WORLDSIZE = 10.0f;
class RtreeDB: public DDSBrick {
private:
	struct Vec3 {
		Vec3() {
		}
		Vec3(float x, float y, float z) {
			v[0] = x;
			v[1] = y;
			v[2] = z;
		}
		Vec3 operator+(const Vec3& other) const {
			return Vec3(v[0] + other.v[0], v[1] + other.v[1], v[2] + other.v[2]);
		}
		void Print() {
			std::cout << "(" << v[0] << "," << v[1] << "," << v[2] << ")" << std::endl;
		}
		float v[3];
	};
	void GenerateObject(Vec3 *min, Vec3 *max) {
		*min = Vec3(RandomFloat(-MAX_WORLDSIZE, MAX_WORLDSIZE),
				RandomFloat(-MAX_WORLDSIZE, MAX_WORLDSIZE),
				RandomFloat(-MAX_WORLDSIZE, MAX_WORLDSIZE));
		Vec3 extent = Vec3(RandomFloat(0.0, FRAC_WORLDSIZE), RandomFloat(0.0, FRAC_WORLDSIZE),
				RandomFloat(0.0, FRAC_WORLDSIZE));
		*max = *min + extent;
	}
public:
	RtreeDB(Ycsb::Core::CoreWorkload &wl) {
		/*
		 * rtreedb is created for each thread to avoid metadata reading/writing
		 */
		rtreeString = NULL;
		keyType = KeyType::CUSTOMIZE;
		MAX_WORLDSIZE = wl.GetMaxKeyValue();
		if (!wl.UseMempoolCache()) {
			DisablePoolCache();
		}
		if (wl.IsSnapshot()) {
			SetSnapshot();
			DisablePoolCache();
		}
	}

	RtreeDB(const RtreeDB& other)
			: DDSBrick(other) {
		std::cout << "rtree copy constructor is called" << std::endl;
		rtreeString = other.rtreeString;
		MAX_WORLDSIZE = other.MAX_WORLDSIZE;
	}

	~RtreeDB() {
		if (rtreeString) {
			delete rtreeString;
		}
	}

	KVDB* Clone(int index) {
		RtreeDB* instance = new RtreeDB(*this);
		std::cout << "Cloning RTree called" << std::endl;
		instance->rtreeString = new RTreeString(false, "rtree.debug" + std::to_string(index),
				poolCached);
		instance->rtreeString->InitDTranxForRTree(DTRANX_SERVER_PORT, instance->ips_,
				instance->selfAddress_, LOCAL_USABLE_PORT_START, true, false, snapshotTranx,
				clients_);
		return instance;
	}

	/*
	 * Not using unordered_map for clients because incompatibility between g++4.6 and g++4.9
	 * now it's already been upgraded to g++4.9
	 */
	void Init(std::vector<std::string> ips, std::string selfAddress, int localStartPort,
			bool fristTime) {
		selfAddress_ = selfAddress;
		ips_ = ips;
		std::shared_ptr<zmq::context_t> context = std::make_shared<zmq::context_t>(1);
		for (auto it = ips.begin(); it != ips.end(); ++it) {
			clients_[*it] = new DTranx::Client::Client(*it, DTRANX_SERVER_PORT, context);
			assert(clients_[*it]->Bind(selfAddress, localStartPort++));
		}
		if (fristTime) {
			rtreeString = new RTreeString(false, "rtree.debug", poolCached);
			rtreeString->InitDTranxForRTree(DTRANX_SERVER_PORT, ips, selfAddress,
					DTRANX_SERVER_PORT, true, true, snapshotTranx, clients_);
		}
	}

	void Close() {
		for (auto it = clients_.begin(); it != clients_.end(); ++it) {
			delete it->second;
		}
	}

	int Read() {
		Vec3 min, max;
		GenerateObject(&min, &max);
		rtreeString->Search(min.v, max.v, &QueryResultCallback, NULL);
		return kOK;
	}

	int ReadSnapshot() {
		Vec3 min, max;
		GenerateObject(&min, &max);
		rtreeString->Search(min.v, max.v, &QueryResultCallback, NULL);
		return kOK;
	}

	int Update() {
		RTree::Core::StringDataType deleteData;
		Vec3 min, max;
		GenerateObject(&min, &max);
		rtreeString->Remove(min.v, max.v, deleteData);
		return kOK;
	}

	int ReadWrite() {
		return kOK;
	}

	int Insert() {
		RTree::Core::StringDataType newData;
		Vec3 min, max;
		GenerateObject(&min, &max);
		rtreeString->Insert(min.v, max.v, newData);
		return kOK;
	}

private:

	float RandomFloat(float min = 0.0, float max = 1.0) {
		std::pair<float, float> range(min, max);
		Util::RandFloatSeed* floatSeed = NULL;
		if (randFloatSeeds.find(range) == randFloatSeeds.end()) {
			floatSeed = randFloatSeeds[range] = new Util::RandFloatSeed(min, max);
		} else {
			floatSeed = randFloatSeeds[range];
		}
		return floatSeed->Next();
	}
	RTreeString *rtreeString;
	std::unordered_map<std::pair<float, float>, Util::RandFloatSeed*, Util::PairHash<float, float>> randFloatSeeds;
	float MAX_WORLDSIZE;
};
} // DB
} // Ycsb

#endif /* YCSB_C_RTREE_DB_H_ */
