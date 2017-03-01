/*
 * Author: Ning Gao(nigo9731@colorado.edu)
 *
 * BTreeDB instance is not shared among threads but BTree Clients are shared
 */

#ifndef YCSB_C_BTREE_DB_H_
#define YCSB_C_BTREE_DB_H_

#include "kvdb.h"
#include "BTree/Core/BTreeTemplate.h"
#include "DB/commons.h"

namespace Ycsb {

using std::cout;
using std::endl;
typedef BTree::Core::BTreeTemplate<uint64_t> BTreeInt;

namespace DB {
class BtreeDB: public KVDB {
public:
	BtreeDB() {
		/*
		 * btreedb is created for each thread to avoid metadata reading/writing
		 */
		btreeInt = NULL;
		shareDB = false;
		keyTypeString = false;
	}

	BtreeDB(const BtreeDB& other) {
		std::cout << "btree copy constructor is called" << std::endl;
		ips_ = other.ips_;
		selfAddress_ = other.selfAddress_;
		clients_ = other.clients_;
		btreeInt = other.btreeInt;
		keyTypeString = other.keyTypeString;
		shareDB = other.shareDB;
	}

	~BtreeDB() {
		if (btreeInt) {
			delete btreeInt;
		}
	}

	KVDB* Clone(int index) {
		BtreeDB* instance = new BtreeDB(*this);
		std::cout << "Cloning BTree called" << std::endl;
		instance->btreeInt = new BTreeInt(false,
				"btree.debug" + std::to_string(index));
		instance->btreeInt->InitDTranxForBTree(DTRANX_SERVER_PORT,
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
			btreeInt = new BTreeInt(false);
			btreeInt->InitDTranxForBTree(DTRANX_SERVER_PORT, ips, selfAddress,
					DTRANX_SERVER_PORT, true, true, clients_);
		}
	}

	void Close() {
		for (auto it = clients_.begin(); it != clients_.end(); ++it) {
			delete it->second;
		}
	}

	int Read(std::vector<uint64_t> keys) {
		std::string value;
		assert(!keys.empty());
		btreeInt->find_unique(keys[0]);
		return kOK;
	}

	int ReadSnapshot(std::vector<uint64_t> keys) {
		return kOK;
	}

	int Update(std::vector<KVPairInt> writes) {
		return kOK;
	}

	int ReadWrite(std::vector<uint64_t> reads, std::vector<KVPairInt> writes) {
		return kOK;
	}

	int Insert(std::vector<KVPairInt> writes) {
		//std::cout << "inserting " << writes[0].first << std::endl;
		assert(!writes.empty());
		btreeInt->insert_unique(writes[0].first);
		return kOK;
	}

private:
	std::unordered_map<std::string, DTranx::Client::Client*> clients_;
	std::vector<std::string> ips_;
	std::string selfAddress_;
	BTreeInt *btreeInt;
};
} // DB
} // Ycsb

#endif /* YCSB_C_BTREE_DB_H_ */
