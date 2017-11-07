/*
 * bang_db.h
 *
 *  Created on: Nov 6, 2017
 *      Author: neal
 */

#ifndef BANG_DB_H_
#define BANG_DB_H_

#include "DB/commons.h"
namespace Ycsb {
namespace DB {

class BangDB: public KVDB {
public:
	BangDB(){
		shareDB = false;
		keyType = KeyType::STRING;
	}

	BangDB(const BangDB& other) {
		std::cout << "BangDB copy contructor is called" << std::endl;
		ips_ = other.ips_;
		shareDB = other.shareDB;
		keyType = other.keyType;
	}

	~BangDB() {
		Close();
	}

	KVDB* Clone(int index) {
		BangDB *instance = new BangDB(*this);
		std::cout << "Cloning BangDB called" << std::endl;
		return instance;
	}

	void Init(std::vector<std::string> ips, std::string selfAddress, int localStartPort,
			bool fristTime) {
		ips_ = ips;
		assert(ips_.size() > 0);
	}

	void Close() {
	}

	int Read(std::vector<std::string> keys) {
	}

	int ReadSnapshot(std::vector<std::string> keys) {
		/*
		 * read snapshot is not supported by bangdb yet
		 */
		return kOK;
	}

	int Update(std::vector<KVPair> writes) {
	}

	int ReadWrite(std::vector<std::string> reads, std::vector<KVPair> writes) {
	}

	int Insert(std::vector<KVPair> writes) {
	}
private:
	std::vector<std::string> ips_;
};

} // DB
} // Ycsb

#endif /* BANG_DB_H_ */
