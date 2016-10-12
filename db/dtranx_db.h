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
		CurClientSetIndex = 0;
		ClientSetNum = 10;
	}

	DtranxDB(const DtranxDB& other) {
		std::cout << "DtranxDB copy contructor is called" << std::endl;
		servers_ = other.servers_;
		clients_ = other.clients_;
		CurClientSetIndex = other.CurClientSetIndex;
		ClientSetNum = other.ClientSetNum;
	}

	KVDB* Clone() {
		std::cout << "DTranxDB clone called" << std::endl;
		return new DtranxDB(*this);
	}

	//Not using unordered_map for clients because incompatibility between g++4.6 and g++4.9
	void Init(std::vector<std::string> servers, std::string selfAddress) {
		std::shared_ptr<zmq::context_t> context = std::make_shared<
				zmq::context_t>(100);
		selfAddress_ = selfAddress;
		servers_ = servers;
		std::vector<std::vector<int> > localPorts;
		localPorts.push_back(std::vector<int>());
		int startPort = 30030;
		for (auto it = servers.begin(); it != servers.end(); ++it) {
			localPorts.back().push_back(startPort++);
		}
		localPorts.push_back(std::vector<int>());
		for (auto it = servers.begin(); it != servers.end(); ++it) {
			localPorts.back().push_back(startPort++);
		}

		std::vector<std::string> remotePorts;
		remotePorts.push_back("30000");
		remotePorts.push_back("30001");
		CurClientSetIndex = 0;
		ClientSetNum = 2;
		for (int curClientSetIndex = 0; curClientSetIndex < remotePorts.size();
				++curClientSetIndex) {
			std::vector<DTranx::Client::Client*> ClientSet;
			for (int serverIndex = 0; serverIndex < servers_.size();
					++serverIndex) {
				std::cout << "creating clients connecting to ip "
						<< servers_[serverIndex] << " port "
						<< remotePorts[curClientSetIndex] << std::endl;
				ClientSet.push_back(
						new DTranx::Client::Client(servers_[serverIndex],
								remotePorts[curClientSetIndex], context));
				std::cout << "creating clients binding to ip " << selfAddress
						<< " port "
						<< localPorts[curClientSetIndex][serverIndex]
						<< std::endl;
				assert(
						ClientSet.back()->Bind(selfAddress,
								std::to_string(
										localPorts[curClientSetIndex][serverIndex])));
			}
			clients_.push_back(ClientSet);
		}
	}
	DTranx::Client::ClientTranx *CreateClientTranx() {
		/*
		 * context, selfAddress, port will never be used in ClientTranx
		 */
		DTranx::Client::ClientTranx *clientTranx =
				new DTranx::Client::ClientTranx("30000", NULL, servers_,
						selfAddress_, "30080");
		assert(servers_.size() == clients_[CurClientSetIndex].size());
		for (size_t i = 0; i < clients_[CurClientSetIndex].size(); ++i) {
			clientTranx->InitClients(servers_[i],
					clients_[CurClientSetIndex][i]);
		}
		CurClientSetIndex = (CurClientSetIndex + 1) % ClientSetNum;
		return clientTranx;
	}

	void DestroyClientTranx(DTranx::Client::ClientTranx *clientTranx) {
		delete clientTranx;
	}

	void Close() {
		for (auto it = clients_.begin(); it != clients_.end(); ++it) {
			for (auto it_b = it->begin(); it_b != it->end(); ++it_b) {
				delete *it_b;
			}
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
	/*
	 * clients_ is a vector of vector of Clients, each outside vector is a list of clients that connects to a certain port
	 * to the servers. dtranx_db basically uses each set of the clients to make the full of the network bandwidth.
	 */
	std::vector<std::vector<DTranx::Client::Client*>> clients_;
	std::vector<std::string> servers_;
	std::string selfAddress_;
	int CurClientSetIndex;
	int ClientSetNum;
};

} // ycsbc

#endif /* YCSB_C_DTRANX_DB_H_ */
