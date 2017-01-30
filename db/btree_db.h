/*
 * Author: Ning Gao(nigo9731@colorado.edu)
 *
 * BTreeDB instance is not shared among threads but BTree Clients are shared
 */

#ifndef YCSB_C_BTREE_DB_H_
#define YCSB_C_BTREE_DB_H_

#include "db/kvdb.h"
#include "BTree/Core/BTreeTemplate.h"
#include "commons.h"

namespace ycsbc {

using std::cout;
using std::endl;
using namespace BTree;
typedef Core::BTreeTemplate<uint64_t> BTreeInt;

class BtreeDB: public KVDB {
public:
	BtreeDB() {
		/*
		 * btreedb is created for each thread to avoid metadata reading/writing
		 */
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
		if (btreeInt) {
			delete btreeInt;
		}
	}

	KVDB* Clone() {
		BtreeDB* instance = new BtreeDB(*this);
		std::cout << "Cloning BTree called" << std::endl;
		/*
		 * dtranxHelper will be reclaimed by BTree class
		 */
		Util::DtranxHelper *dtranxHelper = new Util::DtranxHelper(DTRANX_SERVER_PORT,
				instance->ips_, instance->selfAddress_, LOCAL_USABLE_PORT_START);
		for (size_t i = 0; i < instance->clients_.size(); ++i) {
			dtranxHelper->InitClients(instance->ips_[i], instance->clients_[i]);
		}
		instance->btreeInt = new BTreeInt(dtranxHelper);
		return instance;
	}

	/*
	 * Not using unordered_map for clients because incompatibility between g++4.6 and g++4.9
	 * now it's already been upgraded to g++4.9
	 */
	void Init(std::vector<std::string> ips, std::string selfAddress,
			int localStartPort) {
		selfAddress_ = selfAddress;
		ips_ = ips;
		std::shared_ptr<zmq::context_t> context = std::make_shared<
				zmq::context_t>(1);
		for (auto it = ips.begin(); it != ips.end(); ++it) {
			clients_.push_back(new DTranx::Client::Client(*it, DTRANX_SERVER_PORT, context));
			assert(clients_.back()->Bind(selfAddress, localStartPort++));
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

	int Update(std::vector<KVPair> writes) {
		return kOK;
	}

	int ReadWrite(std::vector<std::string> reads, std::vector<KVPair> writes) {
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
