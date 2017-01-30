/*
 * Author: Ning Gao(nigo9731@colorado.edu)
 */

#ifndef YCSB_C_DTRANX_DB_H_
#define YCSB_C_DTRANX_DB_H_

#include <mutex>
#include <iostream>
#include <fstream>
#include "DTranx/Client/ClientTranx.h"
#include "db/kvdb.h"
#include "db/commons.h"

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
		cout << "DtranxDB copy contructor is called" << endl;
		servers_ = other.servers_;
		clients_ = other.clients_;
		CurClientSetIndex = other.CurClientSetIndex;
		ClientSetNum = other.ClientSetNum;
	}

	KVDB* Clone() {
		cout << "DTranxDB clone called" << endl;
		return new DtranxDB(*this);
	}

	/*
	 * Not using unordered_map for clients because incompatibility between g++4.6 and g++4.9
	 * now it's already been upgraded to g++4.9
	 */
	void Init(std::vector<std::string> servers, std::string selfAddress,
			int localStartPort) {
		std::shared_ptr<zmq::context_t> context = std::make_shared<
				zmq::context_t>(1);
		selfAddress_ = selfAddress;
		servers_ = servers;
		std::vector<std::vector<int> > localPorts;
		localPorts.push_back(std::vector<int>());
		for (auto it = servers.begin(); it != servers.end(); ++it) {
			localPorts.back().push_back(localStartPort++);
		}
		localPorts.push_back(std::vector<int>());
		for (auto it = servers.begin(); it != servers.end(); ++it) {
			localPorts.back().push_back(localStartPort++);
		}
		std::vector<uint32_t> remotePorts;
		remotePorts.push_back(DTRANX_SERVER_PORT);
		CurClientSetIndex = 0;
		ClientSetNum = 1;
		for (uint32_t curClientSetIndex = 0;
				curClientSetIndex < remotePorts.size(); ++curClientSetIndex) {
			std::vector<DTranx::Client::Client*> ClientSet;
			for (uint32_t serverIndex = 0; serverIndex < servers_.size();
					++serverIndex) {
				cout << "creating clients connecting to ip "
						<< servers_[serverIndex] << " port "
						<< remotePorts[curClientSetIndex] << endl;
				ClientSet.push_back(
						new DTranx::Client::Client(servers_[serverIndex],
								remotePorts[curClientSetIndex], context));
				cout << "creating clients binding to ip " << selfAddress
						<< " port "
						<< localPorts[curClientSetIndex][serverIndex] << endl;
				assert(
						ClientSet.back()->Bind(selfAddress,
								localPorts[curClientSetIndex][serverIndex]));
			}
			clients_.push_back(ClientSet);
		}
	}
	DTranx::Client::ClientTranx *CreateClientTranx() {
		/*
		 * context, selfAddress, port will never be used in ClientTranx
		 */
		DTranx::Client::ClientTranx *clientTranx =
				new DTranx::Client::ClientTranx(DTRANX_SERVER_PORT, NULL, servers_,
						selfAddress_, LOCAL_USABLE_PORT_START);
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
		DTranx::Service::GStatus status;

		for (auto it = keys.begin(); it != keys.end(); ++it) {
			status = clientTranx->Read(const_cast<std::string&>(*it), value);
			if (status == DTranx::Service::GStatus::OK) {
			} else {
				DestroyClientTranx(clientTranx);
				cout << "reading " << (*it) << " failure" << endl;
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
			DTranx::Service::GStatus status = clientTranx->Read(
					const_cast<std::string&>(*it), value, true);
			if (status == DTranx::Service::GStatus::OK) {
			} else if (status
					== DTranx::Service::GStatus::SNAPSHOT_NOT_CREATED) {
				DestroyClientTranx(clientTranx);
				cout << "reading " << (*it) << " failure, no snapshot" << endl;
				return kErrorNoData;
			} else {
				DestroyClientTranx(clientTranx);
				cout << "reading " << (*it) << " failure" << endl;
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

	int Update(std::vector<KVPair> writes) {
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

	int ReadWrite(std::vector<std::string> reads, std::vector<KVPair> writes) {
		DTranx::Client::ClientTranx * clientTranx = CreateClientTranx();
		for (auto it = reads.begin(); it != reads.end(); ++it) {
			std::string value;
			DTranx::Service::GStatus status = clientTranx->Read(
					const_cast<std::string&>(*it), value);
			if (status == DTranx::Service::GStatus::OK) {
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
