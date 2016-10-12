/*
 * Author: Ning Gao(nigo9731@colorado.edu)
 */

#ifndef DTRANX_HYPERDEX_DB_H_
#define DTRANX_HYPERDEX_DB_H_

#include <hyperdex/client.h>
namespace ycsbc {

static std::string SPACE = "ning";
static int PORT = 7777;

class HyperdexDB: public KVDB {
public:
	HyperdexDB() {
		shareDB = true;
	}

	HyperdexDB(const HyperdexDB& other) {
		std::cout << "HyperdexDB copy contructor is called" << std::endl;
		ips_ = other.ips_;
		client_ = other.client_;
	}

	KVDB* Clone() {
		return new HyperdexDB(*this);
	}

	void Init(std::vector<std::string> ips, std::string selfAddress = "") {
		ips_ = ips;
		assert(ips_.size() > 0);
		client_ = hyperdex_client_create(ips[0].c_str(), PORT);
	}

	void Close() {
		hyperdex_client_destroy(client_);
	}

	int Read(std::vector<std::string> keys) {
		std::unique_lock<std::mutex> lockGuard(mutex);
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

	int Write(std::vector<KVPair> writes) {
		std::unique_lock<std::mutex> lockGuard(mutex);
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

	int Update(std::vector<std::string> reads, std::vector<KVPair> writes) {
		std::unique_lock<std::mutex> lockGuard(mutex);
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
			hyperdex_client_xact_put(tranx, SPACE.c_str(), it->first.c_str(),
					it->first.size(), &attr, 1, &op_status);
			hyperdex_client_loop(client_, -1, &loop_status);
			if (op_status != HYPERDEX_CLIENT_SUCCESS) {
				//std::cout << "writing " << (it->first) << " failure" << std::endl;
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
		std::unique_lock<std::mutex> lockGuard(mutex);
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
	std::mutex mutex;
	struct hyperdex_client* client_;
	std::vector<std::string> ips_;
};

} // ycsbc

#endif /* DTRANX_HYPERDEX_DB_H_ */
