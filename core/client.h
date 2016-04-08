//
//  client.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <string>
#include "db.h"
#include "db/dtranx_db.h"
#include "core_workload.h"
#include "utils.h"

namespace ycsbc {

class Client {
public:

	Client(CoreWorkload &wl, bool isKV) :
			db_(NULL), dtranx_db_(NULL), workload_(wl), clientID(0) {
	}
	//clientID is used as the output filename. special value -1 means no output
	Client(DB *db_, DtranxDB *dtranx_db_, CoreWorkload &wl, int clientID) :
			db_(db_), dtranx_db_(dtranx_db_), workload_(wl), clientID(clientID) {
		if (clientID != -1) {
			output.open("workload" + std::to_string(clientID));
		}
	}
	void InitDB(DB *db) {
		db_ = db;
	}
	void InitDTranxDB(DtranxDB *dtranx_db) {
		dtranx_db_ = dtranx_db;
	}

	virtual bool DoInsert();
	virtual bool DoTransaction();

	virtual ~Client() {
		if (clientID != -1) {
			output.close();
		}
	}

protected:

	virtual int TransactionRead();
	virtual int TransactionSnapshotRead();
	virtual int TransactionReadModifyWrite();
	virtual int TransactionScan();
	virtual int TransactionUpdate();
	virtual int TransactionInsert();

	DB *db_;
	DtranxDB *dtranx_db_;
	CoreWorkload &workload_;
	int clientID;
	std::ofstream output;
};

inline bool Client::DoInsert() {
	if (dtranx_db_ != NULL) {
		std::vector<DB::KVPair> kvs = workload_.NextTransactionKVs();
		return dtranx_db_->Insert(kvs) == DB::kOK;
	}
	assert(db_ != NULL);
	std::string key = workload_.NextSequenceKey();
	std::vector<DB::KVPair> pairs;
	workload_.BuildValues(pairs);
	return (db_->Insert(workload_.NextTable(), key, pairs) == DB::kOK);
}

inline bool Client::DoTransaction() {
	int status = -1;
	if (workload_.IsSnapshot()) {
		status = TransactionSnapshotRead();
	} else {
		switch (workload_.NextOperation()) {
		case READ:
			status = TransactionRead();
			break;
		case UPDATE:
			status = TransactionUpdate();
			break;
		case INSERT:
			status = TransactionInsert();
			break;
		case SCAN:
			status = TransactionScan();
			break;
		case READMODIFYWRITE:
			status = TransactionReadModifyWrite();
			break;
		default:
			throw utils::Exception("Operation request is not recognized!");
		}
	}
	assert(status >= 0);
	return (status == DB::kOK);
}

inline int Client::TransactionRead() {
	if (dtranx_db_ != NULL) {
		assert(dtranx_db_ != NULL);
		std::vector<std::string> keys = workload_.NextTransactionKeys();
		std::string line = "read ";
		for (auto it = keys.begin(); it != keys.end(); ++it) {
			line += (*it) + " ";
		}
		line += "\n";
		if (clientID != -1) {
			output << line;
		}
		return dtranx_db_->Read(keys);
	}
	assert(db_ != NULL);
	const std::string &table = workload_.NextTable();
	const std::string &key = workload_.NextTransactionKey();
	std::vector<DB::KVPair> result;
	if (!workload_.read_all_fields()) {
		std::vector<std::string> fields;
		fields.push_back("field" + workload_.NextFieldName());
		return db_->Read(table, key, &fields, result);
	} else {
		return db_->Read(table, key, NULL, result);
	}
}
inline int Client::TransactionSnapshotRead() {
	assert(dtranx_db_ != NULL);
	std::vector<std::string> keys = workload_.NextTransactionKeys();
	std::string line = "snapshot read ";
	for (auto it = keys.begin(); it != keys.end(); ++it) {
		line += (*it) + " ";
	}
	line += "\n";
	if (clientID != -1) {
		output << line;
	}
	return dtranx_db_->ReadSnapshot(keys);
}

inline int Client::TransactionReadModifyWrite() {
	if (dtranx_db_ != NULL) {
		assert(dtranx_db_ != NULL);
		std::vector<std::string> keys = workload_.NextTransactionKeys();
		keys.pop_back();
		std::string line = "update ";
		for (auto it = keys.begin(); it != keys.end(); ++it) {
			line += (*it) + " ";
		}
		line += "after ";
		std::vector<DB::KVPair> kvs = workload_.NextTransactionKVs();
		std::vector<DB::KVPair> kvs_filter;
		kvs_filter.push_back(kvs[0]);

		for (auto it = kvs_filter.begin(); it != kvs_filter.end(); ++it) {
			line += (it->first) + " ";
			line += (it->second) + " ";
		}
		line += "\n";
		if (clientID != -1) {
			output << line;
		}
		return dtranx_db_->Update(keys, kvs_filter);
	}
	assert(db_ != NULL);
	const std::string &table = workload_.NextTable();
	const std::string &key = workload_.NextTransactionKey();
	std::vector<DB::KVPair> result;

	if (!workload_.read_all_fields()) {
		std::vector<std::string> fields;
		fields.push_back("field" + workload_.NextFieldName());
		db_->Read(table, key, &fields, result);
	} else {
		db_->Read(table, key, NULL, result);
	}

	std::vector<DB::KVPair> values;
	if (workload_.write_all_fields()) {
		workload_.BuildValues(values);
	} else {
		workload_.BuildUpdate(values);
	}
	return db_->Update(table, key, values);
}

inline int Client::TransactionScan() {
	assert(db_ != NULL);
	const std::string &table = workload_.NextTable();
	const std::string &key = workload_.NextTransactionKey();
	int len = workload_.NextScanLength();
	std::vector<std::vector<DB::KVPair>> result;
	if (!workload_.read_all_fields()) {
		std::vector<std::string> fields;
		fields.push_back("field" + workload_.NextFieldName());
		return db_->Scan(table, key, len, &fields, result);
	} else {
		return db_->Scan(table, key, len, NULL, result);
	}
}

inline int Client::TransactionUpdate() {
	if (dtranx_db_ != NULL) {
		assert(dtranx_db_ != NULL);
		std::vector<DB::KVPair> kvs = workload_.NextTransactionKVs();
		return dtranx_db_->Write(kvs);
	}
	assert(db_ != NULL);
	const std::string &table = workload_.NextTable();
	const std::string &key = workload_.NextTransactionKey();
	std::vector<DB::KVPair> values;
	if (workload_.write_all_fields()) {
		workload_.BuildValues(values);
	} else {
		workload_.BuildUpdate(values);
	}
	return db_->Update(table, key, values);
}

inline int Client::TransactionInsert() {
	if (dtranx_db_ != NULL) {
		assert(dtranx_db_ != NULL);
		std::vector<DB::KVPair> kvs = workload_.NextTransactionKVs();
		return dtranx_db_->Insert(kvs);
	}
	assert(db_ != NULL);
	const std::string &table = workload_.NextTable();
	const std::string &key = workload_.NextSequenceKey();
	std::vector<DB::KVPair> values;
	workload_.BuildValues(values);
	return db_->Insert(table, key, values);
}

} // ycsbc

#endif // YCSB_C_CLIENT_H_
