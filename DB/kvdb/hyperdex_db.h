/*
 * Author: Ning Gao(nigo9731@colorado.edu)
 *
 * Hyperdex DB client Handler
 *  connect to the coordinator not the other slave nodes
 *
 * default table has one attribute called "value"
 * space:
 * key: {"value": "valuestr"}
 */

#ifndef DTRANX_HYPERDEX_DB_H_
#define DTRANX_HYPERDEX_DB_H_

#include <hyperdex/client.h>
#include "DB/commons.h"
namespace Ycsb {
namespace DB {

static std::string SPACE = "ning";
static std::string ATTR = "value";

class HyperdexDB: public KVDB {
public:
	HyperdexDB() :
			client_() {
		shareDB = false;
		keyTypeString = true;
	}

	HyperdexDB(const HyperdexDB& other) {
		std::cout << "HyperdexDB copy contructor is called" << std::endl;
		ips_ = other.ips_;
		shareDB = other.shareDB;
		keyTypeString = other.keyTypeString;
		client_ = NULL;
	}

	~HyperdexDB() {
		Close();
	}

	KVDB* Clone(int index) {
		HyperdexDB *instance = new HyperdexDB(*this);
		std::cout << "Cloning HyperdexDB called" << std::endl;
		instance->client_ = hyperdex_client_create(ips_[0].c_str(),
				HYPERDEX_SERVER_PORT);
		return instance;
	}

	void Init(std::vector<std::string> ips, std::string selfAddress,
			int localStartPort, bool fristTime) {
		ips_ = ips;
		assert(ips_.size() > 0);
		client_ = hyperdex_client_create(ips[0].c_str(), HYPERDEX_SERVER_PORT);
	}

	void Close() {
		if (client_) {
			hyperdex_client_destroy(client_);
		}
	}

	int Read(std::vector<std::string> keys) {
		enum hyperdex_client_returncode tranx_status;
		enum hyperdex_client_returncode loop_status;
		struct hyperdex_client_transaction* tranx =
				hyperdex_client_begin_transaction(client_);

		struct hyperdex_client_attribute* attrs;
		size_t attrs_sz = 0;
		for (auto it = keys.begin(); it != keys.end(); ++it) {
			hyperdex_client_xact_get(tranx, SPACE.c_str(), it->c_str(),
					it->size(), &tranx_status,
					(const hyperdex_client_attribute**) &attrs, &attrs_sz);
			hyperdex_client_loop(client_, -1, &loop_status);
			if (tranx_status != HYPERDEX_CLIENT_SUCCESS) {
				/*
				 std::cout << "reading " << (*it) << " failure "
				 << hyperdex_client_returncode_to_string(tranx_status)
				 << " "
				 << hyperdex_client_returncode_to_string(loop_status)
				 << std::endl;
				 */
				hyperdex_client_abort_transaction(tranx);
				return kErrorNoData;
			}
			hyperdex_client_destroy_attrs(attrs, attrs_sz);
		}
		hyperdex_client_commit_transaction(tranx, &tranx_status);
		hyperdex_client_loop(client_, -1, &loop_status);

		if (tranx_status == HYPERDEX_CLIENT_COMMITTED
				|| tranx_status == HYPERDEX_CLIENT_SUCCESS) {
			return kOK;
		} else {
			return kErrorConflict;
		}
	}

	int ReadSnapshot(std::vector<std::string> keys) {
		/*
		 * read snapshot is not supported by hyperdex yet
		 */
		return kOK;
	}

	int Update(std::vector<KVPair> writes) {
		enum hyperdex_client_returncode tranx_status;
		enum hyperdex_client_returncode loop_status;
		struct hyperdex_client_transaction* tranx =
				hyperdex_client_begin_transaction(client_);

		enum hyperdex_client_returncode op_status;
		struct hyperdex_client_attribute attr;

		for (auto it = writes.begin(); it != writes.end(); ++it) {
			hyperdex_client_xact_put(tranx, SPACE.c_str(), it->first.c_str(),
					it->first.size(), &attr, 1, &op_status);
			hyperdex_client_loop(client_, -1, &loop_status);
		}
		hyperdex_client_commit_transaction(tranx, &tranx_status);
		hyperdex_client_loop(client_, -1, &loop_status);

		if (tranx_status == HYPERDEX_CLIENT_COMMITTED
				|| tranx_status == HYPERDEX_CLIENT_SUCCESS) {
			return kOK;
		} else {
			return kErrorConflict;
		}
	}

	int ReadWrite(std::vector<std::string> reads, std::vector<KVPair> writes) {
		enum hyperdex_client_returncode tranx_status;
		enum hyperdex_client_returncode loop_status;
		enum hyperdex_client_returncode op_status;
		struct hyperdex_client_transaction* tranx =
				hyperdex_client_begin_transaction(client_);

		struct hyperdex_client_attribute attr;
		struct hyperdex_client_attribute* attrs;
		size_t attrs_sz = 0;

		for (auto it = reads.begin(); it != reads.end(); ++it) {
			hyperdex_client_xact_get(tranx, SPACE.c_str(), it->c_str(),
					it->size(), &tranx_status,
					(const hyperdex_client_attribute**) &attrs, &attrs_sz);
			hyperdex_client_loop(client_, -1, &loop_status);
			if (tranx_status != HYPERDEX_CLIENT_SUCCESS) {
				//std::cout << "reading " << (*it) << " failure" << std::endl;
				hyperdex_client_abort_transaction(tranx);
				return kErrorNoData;
			}
			hyperdex_client_destroy_attrs(attrs, attrs_sz);
		}
		for (auto it = writes.begin(); it != writes.end(); ++it) {
			attr.attr = ATTR.c_str();
			attr.value = it->second.c_str();
			attr.datatype = hyperdatatype::HYPERDATATYPE_STRING;
			attr.value_sz = it->second.size();
			hyperdex_client_xact_put(tranx, SPACE.c_str(), it->first.c_str(),
					it->first.size(), &attr, 1, &op_status);
			hyperdex_client_loop(client_, -1, &loop_status);
			if (op_status != HYPERDEX_CLIENT_SUCCESS) {
				/*
				 std::cout << "writing " << (it->first) << " failure"
				 << std::endl;
				 */
				hyperdex_client_abort_transaction(tranx);
				return kErrorNoData;
			}
		}
		hyperdex_client_commit_transaction(tranx, &tranx_status);
		hyperdex_client_loop(client_, -1, &loop_status);

		if (tranx_status == HYPERDEX_CLIENT_COMMITTED
				|| tranx_status == HYPERDEX_CLIENT_SUCCESS) {
			return kOK;
		} else {
			return kErrorConflict;
		}
	}

	int Insert(std::vector<KVPair> writes) {
		enum hyperdex_client_returncode tranx_status;
		enum hyperdex_client_returncode loop_status;
		struct hyperdex_client_transaction* tranx =
				hyperdex_client_begin_transaction(client_);

		enum hyperdex_client_returncode op_status;
		struct hyperdex_client_attribute attr;

		for (auto it = writes.begin(); it != writes.end(); ++it) {
			hyperdex_client_xact_put(tranx, SPACE.c_str(), it->first.c_str(),
					it->first.size(), &attr, 1, &op_status);
			hyperdex_client_loop(client_, -1, &loop_status);
		}
		hyperdex_client_commit_transaction(tranx, &tranx_status);
		hyperdex_client_loop(client_, -1, &loop_status);

		if (tranx_status == HYPERDEX_CLIENT_COMMITTED
				|| tranx_status == HYPERDEX_CLIENT_SUCCESS) {
			return kOK;
		} else {
			return kErrorConflict;
		}
	}
private:
	struct hyperdex_client* client_;
	std::vector<std::string> ips_;
};
} // DB
} // Ycsb

#endif /* DTRANX_HYPERDEX_DB_H_ */
