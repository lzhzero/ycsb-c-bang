/*
 * rtree_db.h
 *
 *  Created on: Mar 1, 2017
 *      Author: neal
 */

#ifndef YCSB_C_RTREE_DB_H_
#define YCSB_C_RTREE_DB_H_

#include "kvdb.h"
#include "RTree/Core/RTree.h"
#include "DB/commons.h"
#include "Util/funcs.h"

namespace Ycsb {
using std::cout;
using std::endl;
typedef RTree::Core::RTree<RTree::Core::StringDataType, float, 3, float, 30, 5> RTreeString;

/*
 * Generate workload based on MAX_WORLDSIZE and FRAC_WORLDSIZE
 * TODO: move the workload generation to core_workload class
 */
const float MAX_WORLDSIZE = 100000000.0f;
const float FRAC_WORLDSIZE = 10.0f;

bool QueryResultCallback(RTree::Core::StringDataType a_data, void* a_context) {
	return true;
}

namespace DB {
class RtreeDB: public KVDB {
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
			std::cout << "(" << v[0] << "," << v[1] << "," << v[2] << ")"
					<< std::endl;
		}
		float v[3];
	};
	void GenerateObject(Vec3 *min, Vec3 *max) {
		*min = Vec3(Util::RandomFloat(-MAX_WORLDSIZE, MAX_WORLDSIZE),
				Util::RandomFloat(-MAX_WORLDSIZE, MAX_WORLDSIZE),
				Util::RandomFloat(-MAX_WORLDSIZE, MAX_WORLDSIZE));
		Vec3 extent = Vec3(Util::RandomFloat(0.0, FRAC_WORLDSIZE),
				Util::RandomFloat(0.0, FRAC_WORLDSIZE),
				Util::RandomFloat(0.0, FRAC_WORLDSIZE));
		*max = *min + extent;
	}
public:
	RtreeDB() {
		/*
		 * rtreedb is created for each thread to avoid metadata reading/writing
		 */
		rtreeString = NULL;
		shareDB = false;
		keyType = KeyType::CUSTOMIZE;
	}

	RtreeDB(const RtreeDB& other) {
		std::cout << "rtree copy constructor is called" << std::endl;
		ips_ = other.ips_;
		selfAddress_ = other.selfAddress_;
		clients_ = other.clients_;
		rtreeString = other.rtreeString;
		keyType = other.keyType;
		shareDB = other.shareDB;
	}

	~RtreeDB() {
		if (rtreeString) {
			delete rtreeString;
		}
	}

	KVDB* Clone(int index) {
		RtreeDB* instance = new RtreeDB(*this);
		std::cout << "Cloning RTree called" << std::endl;
		instance->rtreeString = new RTreeString(false,
				"rtree.debug" + std::to_string(index));
		instance->rtreeString->InitDTranxForRTree(DTRANX_SERVER_PORT,
				instance->ips_, instance->selfAddress_, LOCAL_USABLE_PORT_START,
				true, false, clients_);
		return instance;
	}

	/*
	 * Not using unordered_map for clients because incompatibility between g++4.6 and g++4.9
	 * now it's already been upgraded to g++4.9
	 */
	void Init(std::vector<std::string> ips, std::string selfAddress,
			int localStartPort, bool fristTime) {
		selfAddress_ = selfAddress;
		ips_ = ips;
		std::shared_ptr<zmq::context_t> context = std::make_shared<
				zmq::context_t>(1);
		for (auto it = ips.begin(); it != ips.end(); ++it) {
			clients_[*it] = new DTranx::Client::Client(*it, DTRANX_SERVER_PORT,
					context);
			assert(clients_[*it]->Bind(selfAddress, localStartPort++));
		}
		if (fristTime) {
			rtreeString = new RTreeString(false, "rtree.debug");
			rtreeString->InitDTranxForRTree(DTRANX_SERVER_PORT, ips,
					selfAddress, DTRANX_SERVER_PORT, true, true, clients_);
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

	int ReadSnapshot(std::vector<uint64_t> keys) {
		return kOK;
	}

	int Update() {
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
	std::unordered_map<std::string, DTranx::Client::Client*> clients_;
	std::vector<std::string> ips_;
	std::string selfAddress_;
	RTreeString *rtreeString;
};
} // DB
} // Ycsb

#endif /* YCSB_C_RTREE_DB_H_ */
