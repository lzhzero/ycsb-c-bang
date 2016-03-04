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
	void Init(std::string clusterFileName) {
		std::lock_guard<std::mutex> lock(mutex_);
		cout << "A new thread begins working." << endl;
		DTranx::Util::ConfigHelper configHelper;
		configHelper.readFile("DTranx.conf");
		std::shared_ptr<zmq::context_t> context = std::make_shared<
				zmq::context_t>(
				configHelper.read<DTranx::uint32>("maxIOThreads"));
		std::vector<std::string> ips;
		if (clusterFileName.empty()) {
			ips.push_back(configHelper.read("SelfAddress"));
		} else {
			std::ifstream ipFile(clusterFileName);
			if (!ipFile.is_open()) {
				cout<< "cannot open "<< clusterFileName<< endl;
				exit(0);
			}
			std::string ip;
			while (std::getline(ipFile, ip)) {
				ips.push_back(ip);
			}
		}
		clientTranx = new DTranx::Client::ClientTranx("DTranx.conf", context,
				ips);
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
				cout << "read key: " << *it << " and the value is " << value
						<< endl;
			} else {
				cout << "read key failed, abort..." << endl;
				clientTranx->Clear();
				return 0;
			}
		}
		bool success = clientTranx->Commit();
		if (success) {
			cout << "commit success" << endl;
		} else {
			cout << "commit failure" << endl;
		}
		clientTranx->Clear();
		return 0;
	}

	int Write(std::vector<KVPair> writes) {
		std::lock_guard<std::mutex> lock(mutex_);
		for (auto it = writes.begin(); it != writes.end(); ++it) {
			clientTranx->Write(it->first, it->second);
			cout << "update write key: " << it->first << " and the value is "
					<< it->second << endl;
		}
		bool success = clientTranx->Commit();
		if (success) {
			cout << "commit success" << endl;
		} else {
			cout << "commit failure" << endl;
		}
		clientTranx->Clear();
		return 0;
	}

	int Update(std::vector<std::string> reads, std::vector<KVPair> writes) {
		std::lock_guard<std::mutex> lock(mutex_);
		for (auto it = reads.begin(); it != reads.end(); ++it) {
			std::string value;
			DTranx::Storage::Status status = clientTranx->Read(
					const_cast<std::string&>(*it), value);
			if (status == DTranx::Storage::Status::OK) {
				cout << "read key: " << *it << " and the value is " << value
						<< endl;
			} else {
				cout << "read key failed, abort..." << endl;
				clientTranx->Clear();
				return 0;
			}
		}
		for (auto it = writes.begin(); it != writes.end(); ++it) {
			clientTranx->Write(it->first, it->second);
			cout << "update write key: " << it->first << " and the value is "
					<< it->second << endl;
		}
		bool success = clientTranx->Commit();
		if (success) {
			cout << "commit success" << endl;
		} else {
			cout << "commit failure" << endl;
		}
		clientTranx->Clear();
		return 0;
	}

	int Insert(std::vector<KVPair> writes) {
		std::lock_guard<std::mutex> lock(mutex_);
		for (auto it = writes.begin(); it != writes.end(); ++it) {
			cout << "insert write key: " << it->first << " and the value is "
					<< it->second << endl;
			clientTranx->Write(it->first, it->second);
		}
		bool success = clientTranx->Commit();
		if (success) {
			cout << "commit success" << endl;
		} else {
			cout << "commit failure" << endl;
		}
		clientTranx->Clear();
		return 0;
	}
private:
	std::mutex mutex_;
	DTranx::Client::ClientTranx *clientTranx;
};

} // ycsbc

#endif /* YCSB_C_DTRANX_DB_H_ */
