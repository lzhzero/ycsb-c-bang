/*
 * Author: Ning Gao(nigo9731@colorado.edu)
 */

#ifndef YCSB_C_BTREE_DB_H_
#define YCSB_C_BTREE_DB_H_

#include "core/kvdb.h"
#include "BTree/Core/BTreeTemplate.h"

namespace ycsbc {

using std::cout;
using std::endl;
using namespace BTree;
typedef Core::BTreeTemplate<uint64_t> BTreeInt;

class BtreeDB: public KVDB {
public:
	BtreeDB() {
		btreeInt = NULL;
		shareDB = false;
	}

	BtreeDB(const BtreeDB& other) {
		std::cout << "btree copy constructor is called" << std::endl;
		ips_ = other.ips_;
		selfAddress_ = other.selfAddress_;
		clients_ = other.clients_;
		btreeInt = other.btreeInt;
	}

	~BtreeDB() {
		DestroyDB();
	}

	KVDB* Clone() {
		BtreeDB* instance = new BtreeDB(*this);
		instance->CreateDB();
		return instance;
	}

	//Not using unordered_map for clients because incompatibility between g++4.6 and g++4.9
	void Init(std::vector<std::string> ips, std::string selfAddress) {
		selfAddress_ = selfAddress;
		ips_ = ips;
		std::shared_ptr<zmq::context_t> context = std::make_shared<
				zmq::context_t>(100);
		int startPort = 30030;
		for (auto it = ips.begin(); it != ips.end(); ++it) {
			clients_.push_back(new DTranx::Client::Client(*it, 30000, context));
			assert(clients_.back()->Bind(selfAddress, startPort++));
		}
	}
	void CreateDB() {
		//TODO: reclaim dtranxHelper
		std::cout << "CreateDB called" << std::endl;
		Util::DtranxHelper *dtranxHelper = new Util::DtranxHelper(30000, ips_,
				selfAddress_, 30080);
		for (size_t i = 0; i < clients_.size(); ++i) {
			dtranxHelper->InitClients(ips_[i], clients_[i]);
		}
		btreeInt = new BTreeInt(dtranxHelper);
	}

	void DestroyDB() {
		if (btreeInt) {
			delete btreeInt;
		}
	}

	void Close() {
		for (auto it = clients_.begin(); it != clients_.end(); ++it) {
			delete *it;
		}
	}

	uint64_t StringKeyToInt(std::string stringKey) {
		return std::stoull(stringKey.substr(4));
	}

	int Read(std::vector<std::string> keys) {
		std::string value;

		assert(!keys.empty());
		uint64_t realKey = StringKeyToInt(keys[0]);

		btreeInt->find_unique(realKey);
		//std::cout << "read key: " << realKey << " "
		//		<< (result ? "true" : "false") << std::endl;
		return kOK;
	}

	int ReadSnapshot(std::vector<std::string> keys) {
		return kOK;
	}

	int Write(std::vector<KVPair> writes) {
		return kOK;
	}

	int Update(std::vector<std::string> reads, std::vector<KVPair> writes) {
		return kOK;
	}

	int Insert(std::vector<KVPair> writes) {
		assert(!writes.empty());
		uint64_t realKey = StringKeyToInt(writes[0].first);
		std::cout << "insert key: " << realKey << std::endl;
		bool result = btreeInt->insert_unique(realKey);
		std::cout << "insert key: " << realKey << " "
				<< (result ? "true" : "false") << std::endl;
		return kOK;
	}

private:
	std::vector<DTranx::Client::Client*> clients_;
	std::vector<std::string> ips_;
	std::string selfAddress_;
	BTreeInt *btreeInt;
};

} // ycsbc

#endif /* YCSB_C_BTREE_DB_H_ */
