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
#include "db/table_db.h"
#include "db/dtranx_db.h"
#include "core_workload.h"
#include "utils.h"

namespace ycsbc {
class Client {
public:

	Client(CoreWorkload &wl) :
			db_(NULL), kv_db_(NULL), workload_(wl), isKV(false) {
	}

	Client(TableDB *db_, KVDB *kv_db, CoreWorkload &wl) :
			db_(db_), kv_db_(kv_db), workload_(wl), isKV(false)  {
		if(kv_db_){
			isKV = true;
		}
	}

	virtual bool DoInsert();
	virtual bool DoTransaction();

	virtual ~Client() {
	}

protected:

	virtual int TransactionRead();
	virtual int TransactionSnapshotRead();
	virtual int TransactionReadModifyWrite();
	virtual int TransactionScan();
	virtual int TransactionUpdate();
	virtual int TransactionInsert();


	TableDB *db_;
	KVDB *kv_db_;
	CoreWorkload &workload_;
	bool isKV;
};

inline bool Client::DoInsert() {
	if (isKV) {
		std::vector<DB_BASE::KVPair> kvs = workload_.NextTransactionKVs();
		return kv_db_->Insert(kvs) == DB_BASE::kOK;
	}
	assert(db_ != NULL);
	std::string key = workload_.NextSequenceKey();
	std::vector<DB_BASE::KVPair> pairs;
	workload_.BuildValues(pairs);
	return (db_->Insert(workload_.NextTable(), key, pairs) == DB_BASE::kOK);
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
	return (status == DB_BASE::kOK);
}

inline int Client::TransactionRead() {
	if (isKV) {
		assert(kv_db_ != NULL);
		std::vector<std::string> keys = workload_.NextTransactionKeys();
		return kv_db_->Read(keys);
	}
	assert(db_ != NULL);
	const std::string &table = workload_.NextTable();
	const std::string &key = workload_.NextTransactionKey();
	std::vector<DB_BASE::KVPair> result;
	if (!workload_.read_all_fields()) {
		std::vector<std::string> fields;
		fields.push_back("field" + workload_.NextFieldName());
		return db_->Read(table, key, &fields, result);
	} else {
		return db_->Read(table, key, NULL, result);
	}
}
inline int Client::TransactionSnapshotRead() {
	assert(kv_db_ != NULL);
	std::vector<std::string> keys = workload_.NextTransactionKeys();
	return kv_db_->ReadSnapshot(keys);
}

inline int Client::TransactionReadModifyWrite() {
	if (isKV) {
		assert(kv_db_ != NULL);
		std::vector<std::string> keys = workload_.NextTransactionKeys();
		keys.pop_back();
		std::string line = "update ";
		for (auto it = keys.begin(); it != keys.end(); ++it) {
			line += (*it) + " ";
		}
		line += "after ";
		std::vector<DB_BASE::KVPair> kvs = workload_.NextTransactionKVs();
		std::vector<DB_BASE::KVPair> kvs_filter;
		kvs_filter.push_back(kvs[0]);
		return kv_db_->ReadWrite(keys, kvs_filter);
	}
	assert(db_ != NULL);
	const std::string &table = workload_.NextTable();
	const std::string &key = workload_.NextTransactionKey();
	std::vector<DB_BASE::KVPair> result;

	if (!workload_.read_all_fields()) {
		std::vector<std::string> fields;
		fields.push_back("field" + workload_.NextFieldName());
		db_->Read(table, key, &fields, result);
	} else {
		db_->Read(table, key, NULL, result);
	}

	std::vector<DB_BASE::KVPair> values;
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
	std::vector<std::vector<DB_BASE::KVPair>> result;
	if (!workload_.read_all_fields()) {
		std::vector<std::string> fields;
		fields.push_back("field" + workload_.NextFieldName());
		return db_->Scan(table, key, len, &fields, result);
	} else {
		return db_->Scan(table, key, len, NULL, result);
	}
}

inline int Client::TransactionUpdate() {
	if (isKV) {
		assert(kv_db_ != NULL);
		std::vector<DB_BASE::KVPair> kvs = workload_.NextTransactionKVs();
		return kv_db_->Update(kvs);
	}
	assert(db_ != NULL);
	const std::string &table = workload_.NextTable();
	const std::string &key = workload_.NextTransactionKey();
	std::vector<DB_BASE::KVPair> values;
	if (workload_.write_all_fields()) {
		workload_.BuildValues(values);
	} else {
		workload_.BuildUpdate(values);
	}
	return db_->Update(table, key, values);
}

inline int Client::TransactionInsert() {
	if (isKV) {
		assert(kv_db_ != NULL);
		std::vector<DB_BASE::KVPair> kvs = workload_.NextTransactionKVs();
		return kv_db_->Insert(kvs);
	}
	assert(db_ != NULL);
	const std::string &table = workload_.NextTable();
	const std::string &key = workload_.NextSequenceKey();
	std::vector<DB_BASE::KVPair> values;
	workload_.BuildValues(values);
	return db_->Insert(table, key, values);
}

} // ycsbc

#endif // YCSB_C_CLIENT_H_
