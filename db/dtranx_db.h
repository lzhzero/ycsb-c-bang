/*
 * Author: Ning Gao(nigo9731@colorado.edu)
 */

#ifndef YCSB_C_DTRANX_DB_H_
#define YCSB_C_DTRANX_DB_H_

#include <mutex>
#include <iostream>
#include <fstream>
#include "DTranx/Client/ClientTranx.h"
#include "core/kvdb.h"

namespace ycsbc {

using std::cout;
using std::endl;

class DtranxDB: public KVDB {
public:
	DtranxDB() {
		shareDB = true;
	}

	DtranxDB(const DtranxDB& other) {
		std::cout << "DtranxDB copy contructor is called" << std::endl;
		ips_ = other.ips_;
		clients_ = other.clients_;
	}

	KVDB* Clone() {
		return new DtranxDB(*this);
	}

	//Not using unordered_map for clients because incompatibility between g++4.6 and g++4.9
	void Init(std::vector<std::string> ips) {
		ips_ = ips;
		std::shared_ptr<zmq::context_t> context = std::make_shared<
				zmq::context_t>(100);
		for (auto it = ips.begin(); it != ips.end(); ++it) {
			clients_.push_back(
					new DTranx::Client::Client(*it, "60000", context));

		}
	}
	DTranx::Client::ClientTranx *CreateClientTranx() {
		DTranx::Client::ClientTranx *clientTranx =
				new DTranx::Client::ClientTranx("60000", NULL, ips_);
		assert(ips_.size() == clients_.size());
		for (size_t i = 0; i < clients_.size(); ++i) {
			clientTranx->InitClients(ips_[i], clients_[i]);
		}
		return clientTranx;
	}

	void DestroyClientTranx(DTranx::Client::ClientTranx *clientTranx) {
		delete clientTranx;
	}

	void Close() {
		for (auto it = clients_.begin(); it != clients_.end(); ++it) {
			delete *it;
		}
	}

	int Read(std::vector<std::string> keys) {
		DTranx::Client::ClientTranx * clientTranx = CreateClientTranx();
		std::string value;
		DTranx::Storage::Status status;

		for (auto it = keys.begin(); it != keys.end(); ++it) {
			status = clientTranx->Read(const_cast<std::string&>(*it), value);
			if (status == DTranx::Storage::Status::OK) {
			} else {
				DestroyClientTranx(clientTranx);
				std::cout << "reading " << (*it) << " failure" << std::endl;
				return kErrorNoData;
			}
		}
		bool success = clientTranx->Commit();
		DestroyClientTranx(clientTranx);
		if (success) {
			return kOK;
		} else {
			return kErrorConflict;
		}
	}

	int ReadSnapshot(std::vector<std::string> keys) {
		DTranx::Client::ClientTranx * clientTranx = CreateClientTranx();
		for (auto it = keys.begin(); it != keys.end(); ++it) {
			std::string value;
			DTranx::Storage::Status status = clientTranx->Read(
					const_cast<std::string&>(*it), value, true);
			if (status == DTranx::Storage::Status::OK) {
			} else if (status
					== DTranx::Storage::Status::SNAPSHOT_NOT_CREATED) {
				DestroyClientTranx(clientTranx);
				std::cout << "reading " << (*it) << " failure, no snapshot"
						<< std::endl;
				return kErrorNoData;
			} else {
				DestroyClientTranx(clientTranx);
				std::cout << "reading " << (*it) << " failure" << std::endl;
				return kErrorNoData;
			}
		}
		bool success = clientTranx->Commit();
		DestroyClientTranx(clientTranx);
		if (success) {
			return kOK;
		} else {
			return kErrorConflict;
		}
	}

	int Write(std::vector<KVPair> writes) {
		DTranx::Client::ClientTranx * clientTranx = CreateClientTranx();
		for (auto it = writes.begin(); it != writes.end(); ++it) {
			clientTranx->Write(it->first, it->second);
			cout << "update write key: " << it->first << " and the value is "
					<< it->second << endl;
		}
		bool success = clientTranx->Commit();
		DestroyClientTranx(clientTranx);
		if (success) {
			return kOK;
		} else {
			return kErrorConflict;
		}
	}

	int Update(std::vector<std::string> reads, std::vector<KVPair> writes) {
		DTranx::Client::ClientTranx * clientTranx = CreateClientTranx();
		for (auto it = reads.begin(); it != reads.end(); ++it) {
			std::string value;
			DTranx::Storage::Status status = clientTranx->Read(
					const_cast<std::string&>(*it), value);
			if (status == DTranx::Storage::Status::OK) {
			} else {
				DestroyClientTranx(clientTranx);
				return kErrorNoData;
			}
		}
		for (auto it = writes.begin(); it != writes.end(); ++it) {
			clientTranx->Write(it->first, it->second);
		}
		bool success = clientTranx->Commit();
		DestroyClientTranx(clientTranx);
		if (success) {
			return kOK;
		} else {
			return kErrorConflict;
		}
	}

	int Insert(std::vector<KVPair> writes) {
		DTranx::Client::ClientTranx * clientTranx = CreateClientTranx();
		for (auto it = writes.begin(); it != writes.end(); ++it) {
			cout << "insert write key: " << it->first << " and the value is "
					<< it->second << endl;
			clientTranx->Write(it->first, it->second);
		}
		bool success = clientTranx->Commit();
		DestroyClientTranx(clientTranx);
		if (success) {
			cout << "commit success" << endl;
			return kOK;
		} else {
			cout << "commit failure" << endl;
			return kErrorConflict;
		}
	}
private:
	std::vector<DTranx::Client::Client*> clients_;
	std::vector<std::string> ips_;
};

} // ycsbc

#endif /* YCSB_C_DTRANX_DB_H_ */
