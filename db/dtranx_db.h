/*
 * Author: Ning Gao(nigo9731@colorado.edu)
 */

#ifndef YCSB_C_DTRANX_DB_H_
#define YCSB_C_DTRANX_DB_H_

#include "core/db.h"

#include <mutex>
#include <iostream>
#include <fstream>
#include "DTranx/Client/ClientTranx.h"
#include "DTranx/Util/ConfigHelper.h"

namespace ycsbc {

using std::cout;
using std::endl;

class DtranxDB {
public:
	typedef std::pair<std::string, std::string> KVPair;
	//Not using unordered_map for clients because incompatibility between g++4.6 and g++4.9
	void Init(std::vector<std::string> ips, std::vector<DTranx::Client::Client*> clients) {
		std::lock_guard<std::mutex> lock(mutex_);
		cout << "A new thread begins working." << endl;
		DTranx::Util::ConfigHelper configHelper;
		configHelper.readFile("DTranx.conf");
		std::shared_ptr<zmq::context_t> context = std::make_shared<
				zmq::context_t>(
				configHelper.read<DTranx::uint32>("maxIOThreads"));
		clientTranx = new DTranx::Client::ClientTranx("DTranx.conf", context, ips);
		for(size_t i= 0; i< clients.size(); ++i){
			clientTranx->InitClients(ips[i], clients[i]);
		}
	}

	void Close() {
		if (clientTranx) {
			delete clientTranx;
		}
	}

	int Read(std::vector<std::string> keys) {
		std::lock_guard<std::mutex> lock(mutex_);
		for (auto it = keys.begin(); it != keys.end(); ++it) {
			std::string value;
			DTranx::Storage::Status status = clientTranx->Read(
					const_cast<std::string&>(*it), value);
			if (status == DTranx::Storage::Status::OK) {
				//cout << "read key: " << *it << " and the value is " << value
				//		<< endl;
			} else {
				//cout << "read key: "<<*it<<"  failed, abort..." << endl;
				clientTranx->Clear();
				return DB::kErrorNoData;
			}
		}
		bool success = clientTranx->Commit();
		clientTranx->Clear();
		if (success) {
			//cout << "commit success" << endl;
			return DB::kOK;
		} else {
			//cout << "commit failure" << endl;
			return DB::kErrorConflict;
		}
	}

	int Write(std::vector<KVPair> writes) {
		std::lock_guard<std::mutex> lock(mutex_);
		for (auto it = writes.begin(); it != writes.end(); ++it) {
			clientTranx->Write(it->first, it->second);
			cout << "update write key: " << it->first << " and the value is "
					<< it->second << endl;
		}
		bool success = clientTranx->Commit();
		clientTranx->Clear();
		if (success) {
			cout << "commit success" << endl;
			return DB::kOK;
		} else {
			cout << "commit failure" << endl;
			return DB::kErrorConflict;
		}
	}

	int Update(std::vector<std::string> reads, std::vector<KVPair> writes) {
		std::lock_guard<std::mutex> lock(mutex_);
		for (auto it = reads.begin(); it != reads.end(); ++it) {
			std::string value;
			DTranx::Storage::Status status = clientTranx->Read(
					const_cast<std::string&>(*it), value);
			if (status == DTranx::Storage::Status::OK) {
				//cout << "read key: " << *it << " and the value is " << value
				//		<< endl;
			} else {
				//cout << "read key: "<<*it<<"  failed, abort..." << endl;
				clientTranx->Clear();
				return DB::kErrorNoData;
			}
		}
		for (auto it = writes.begin(); it != writes.end(); ++it) {
			clientTranx->Write(it->first, it->second);
			//cout << "update write key: " << it->first << " and the value is "
			//		<< it->second << endl;
		}
		bool success = clientTranx->Commit();
		clientTranx->Clear();
		if (success) {
			//cout << "commit success" << endl;
			return DB::kOK;
		} else {
			//cout << "commit failure" << endl;
			return DB::kErrorConflict;
		}
	}

	int Insert(std::vector<KVPair> writes) {
		std::lock_guard<std::mutex> lock(mutex_);
		for (auto it = writes.begin(); it != writes.end(); ++it) {
			cout << "insert write key: " << it->first << " and the value is "
					<< it->second << endl;
			clientTranx->Write(it->first, it->second);
		}
		bool success = clientTranx->Commit();
		clientTranx->Clear();
		if (success) {
			cout << "commit success" << endl;
			return DB::kOK;
		} else {
			cout << "commit failure" << endl;
			return DB::kErrorConflict;
		}
	}
private:
	std::mutex mutex_;
	DTranx::Client::ClientTranx *clientTranx;
};

} // ycsbc

#endif /* YCSB_C_DTRANX_DB_H_ */
