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
	//Not using unordered_map for clients because incompatibility between g++4.6 and g++4.9
	void Init(std::vector<std::string> ips){
		ips_ = ips;
		std::shared_ptr<zmq::context_t> context = std::make_shared
				< zmq::context_t > (100);
		for (auto it = ips.begin(); it != ips.end(); ++it) {
			clients_.push_back(
					new DTranx::Client::Client(*it, "60000", context));

		}
	}
	BTreeInt *CreateBTree() {
		Util::DtranxHelper *dtranxHelper = new Util::DtranxHelper("60000", ips_);
		BTreeInt *btreeInt = new BTreeInt(dtranxHelper);
		for (size_t i = 0; i < clients_.size(); ++i) {
			dtranxHelper->InitClients(ips_[i], clients_[i]);
		}
		return btreeInt;
	}

	void DestroyBTree(BTreeInt *btreeInt) {
		delete btreeInt;
	}

	void Close() {
		for (auto it = clients_.begin(); it != clients_.end(); ++it) {
			delete *it;
		}
	}

	uint64_t StringKeyToInt(std::string stringKey){
		return std::stoull(stringKey.substr(4));
	}

	int Read(std::vector<std::string> keys) {
		BTreeInt *btreeInt = CreateBTree();
		std::string value;

		assert(!keys.empty());
		uint64_t realKey = StringKeyToInt(keys[0]);

		bool result = btreeInt->find_unique(realKey);
		std::cout<<"read key: "<<realKey<<" "<<(result?"true":"false") <<std::endl;
		DestroyBTree(btreeInt);
		return kOK;
	}

	int ReadSnapshot(std::vector<std::string> keys) {
		BTreeInt *btreeInt = CreateBTree();
		DestroyBTree(btreeInt);
		return kOK;
	}

	int Write(std::vector<KVPair> writes) {
		return kOK;
	}

	int Update(std::vector<std::string> reads, std::vector<KVPair> writes) {
		return kOK;
	}

	int Insert(std::vector<KVPair> writes) {
		BTreeInt *btreeInt = CreateBTree();
		assert(!writes.empty());
		uint64_t realKey = StringKeyToInt(writes[0].first);
		bool result = btreeInt->insert_unique(realKey);
		std::cout<<"insert key: "<<realKey<<" "<< (result?"true":"false") <<std::endl;
		DestroyBTree(btreeInt);
		return kOK;
	}
private:
	std::vector<DTranx::Client::Client*> clients_;
	std::vector<std::string> ips_;
};

} // ycsbc


#endif /* YCSB_C_BTREE_DB_H_ */
