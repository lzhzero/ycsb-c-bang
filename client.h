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
#include "DB/tabledb/table_db.h"
#include "DB/kvdb/dtranx_db.h"
#include "Core/core_workload.h"
#include "Util/Exception.h"

namespace Ycsb {
class Client {
public:

	Client(Core::CoreWorkload &wl)
			: db_(NULL), kv_db_(NULL), workload_(wl), isKV(false) {
	}

	Client(DB::TableDB *db_, DB::KVDB *kv_db, Core::CoreWorkload &wl)
			: db_(db_), kv_db_(kv_db), workload_(wl), isKV(false) {
		if (kv_db_) {
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

	DB::TableDB *db_;
	DB::KVDB *kv_db_;
	Core::CoreWorkload &workload_;

	bool isKV;
};

inline bool Client::DoInsert() {
	if (isKV) {
		if (kv_db_->GetKeyType() == DB::KVDB::KeyType::STRING) {
			std::vector<DB::DB_BASE::KVPair> kvs = workload_.NextTransactionKVs();
			return kv_db_->Insert(kvs) == DB::DB_BASE::kOK;
		} else if (kv_db_->GetKeyType() == DB::KVDB::KeyType::INTEGER) {
			std::vector<DB::DB_BASE::KVPairInt> kvs = workload_.NextTransactionKVsInt();
			return kv_db_->Insert(kvs) == DB::DB_BASE::kOK;
		} else {
			return kv_db_->Insert() == DB::DB_BASE::kOK;
		}
	}
	assert(db_ != NULL);
	std::string key = workload_.NextSequenceKey();
	std::vector<DB::DB_BASE::KVPair> pairs;
	workload_.BuildValues(pairs);
	return (db_->Insert(workload_.NextTable(), key, pairs) == DB::DB_BASE::kOK);
}

inline bool Client::DoTransaction() {
	int status = -1;
	if (workload_.IsSnapshot()) {
		status = TransactionSnapshotRead();
	} else {
		switch (workload_.NextOperation()) {
		case Core::READ:
			status = TransactionRead();
			break;
		case Core::UPDATE:
			status = TransactionUpdate();
			break;
		case Core::INSERT:
			status = TransactionInsert();
			break;
		case Core::SCAN:
			status = TransactionScan();
			break;
		case Core::READMODIFYWRITE:
			status = TransactionReadModifyWrite();
			break;
		default:
			throw Util::Exception("Operation request is not recognized!");
		}
	}
	assert(status >= 0);
	return (status == DB::DB_BASE::kOK);
}

/*
 * TableDB, KVDB
 */
inline int Client::TransactionRead() {
	if (isKV) {
		assert(kv_db_ != NULL);
		if (kv_db_->GetKeyType() == DB::KVDB::KeyType::STRING) {
			std::vector<std::string> keys = workload_.NextTransactionKeys();
			return kv_db_->Read(keys);
		} else if (kv_db_->GetKeyType() == DB::KVDB::KeyType::INTEGER) {
			std::vector<uint64_t> keys = workload_.NextTransactionKeysInt();
			return kv_db_->Read(keys);
		} else {
			std::vector<uint64_t> keys;
			return kv_db_->Read();
		}
	} else {
		assert(db_ != NULL);
		const std::string &table = workload_.NextTable();
		const std::string &key = workload_.NextTransactionKey();
		std::vector<DB::DB_BASE::KVPair> result;
		if (!workload_.read_all_fields()) {
			std::vector<std::string> fields;
			fields.push_back("field" + workload_.NextFieldName());
			return db_->Read(table, key, &fields, result);
		} else {
			return db_->Read(table, key, NULL, result);
		}
	}
}

/*
 * KVDB
 */
inline int Client::TransactionSnapshotRead() {
	assert(kv_db_ != NULL);

	if (kv_db_->GetKeyType() == DB::KVDB::KeyType::STRING) {
		std::vector<std::string> keys = workload_.NextTransactionKeys();
		return kv_db_->ReadSnapshot(keys);
	} else if (kv_db_->GetKeyType() == DB::KVDB::KeyType::INTEGER) {
		std::vector<uint64_t> keys = workload_.NextTransactionKeysInt();
		return kv_db_->ReadSnapshot(keys);
	} else {
		std::vector<uint64_t> keys;
		return kv_db_->ReadSnapshot();
	}
}

/*
 * TableDB, KVDB
 */
inline int Client::TransactionReadModifyWrite() {
	if (isKV) {
		assert(kv_db_ != NULL);
		if (kv_db_->GetKeyType() == DB::KVDB::KeyType::STRING) {
			std::vector<std::string> keys = workload_.NextTransactionKeys();
			keys.pop_back();
			std::vector<DB::DB_BASE::KVPair> kvs = workload_.NextTransactionKVs();
			std::vector<DB::DB_BASE::KVPair> kvs_filter;
			kvs_filter.push_back(kvs[0]);
			return kv_db_->ReadWrite(keys, kvs_filter);
		} else if (kv_db_->GetKeyType() == DB::KVDB::KeyType::INTEGER) {
			std::vector<uint64_t> keys = workload_.NextTransactionKeysInt();
			keys.pop_back();
			std::vector<DB::DB_BASE::KVPairInt> kvs = workload_.NextTransactionKVsInt();
			std::vector<DB::DB_BASE::KVPairInt> kvs_filter;
			kvs_filter.push_back(kvs[0]);
			return kv_db_->ReadWrite(keys, kvs_filter);
		} else {
			return kv_db_->ReadWrite();
		}
	} else {
		assert(db_ != NULL);
		const std::string &table = workload_.NextTable();
		const std::string &key = workload_.NextTransactionKey();
		std::vector<DB::DB_BASE::KVPair> result;

		if (!workload_.read_all_fields()) {
			std::vector<std::string> fields;
			fields.push_back("field" + workload_.NextFieldName());
			db_->Read(table, key, &fields, result);
		} else {
			db_->Read(table, key, NULL, result);
		}

		std::vector<DB::DB_BASE::KVPair> values;
		if (workload_.write_all_fields()) {
			workload_.BuildValues(values);
		} else {
			workload_.BuildUpdate(values);
		}
		return db_->Update(table, key, values);
	}
}

/*
 * TableDB
 */
inline int Client::TransactionScan() {
	assert(db_ != NULL);
	const std::string &table = workload_.NextTable();
	const std::string &key = workload_.NextTransactionKey();
	int len = workload_.NextScanLength();
	std::vector<std::vector<DB::DB_BASE::KVPair>> result;
	if (!workload_.read_all_fields()) {
		std::vector<std::string> fields;
		fields.push_back("field" + workload_.NextFieldName());
		return db_->Scan(table, key, len, &fields, result);
	} else {
		return db_->Scan(table, key, len, NULL, result);
	}
}

/*
 * TableDB, KVDB
 */
inline int Client::TransactionUpdate() {
	if (isKV) {
		assert(kv_db_ != NULL);
		if (kv_db_->GetKeyType() == DB::KVDB::KeyType::STRING) {
			std::vector<DB::DB_BASE::KVPair> kvs = workload_.NextTransactionKVs();
			return kv_db_->Update(kvs);
		} else if (kv_db_->GetKeyType() == DB::KVDB::KeyType::INTEGER) {
			std::vector<DB::DB_BASE::KVPairInt> kvs = workload_.NextTransactionKVsInt();
			return kv_db_->Update(kvs);
		} else {
			return kv_db_->Update();
		}
	} else {
		assert(db_ != NULL);
		const std::string &table = workload_.NextTable();
		const std::string &key = workload_.NextTransactionKey();
		std::vector<DB::DB_BASE::KVPair> values;
		if (workload_.write_all_fields()) {
			workload_.BuildValues(values);
		} else {
			workload_.BuildUpdate(values);
		}
		return db_->Update(table, key, values);
	}
}

/*
 * TableDB, KVDB
 */
inline int Client::TransactionInsert() {
	if (isKV) {
		assert(kv_db_ != NULL);
		if (kv_db_->GetKeyType() == DB::KVDB::KeyType::STRING) {
			std::vector<DB::DB_BASE::KVPair> kvs = workload_.NextTransactionKVs();
			return kv_db_->Insert(kvs);
		} else if (kv_db_->GetKeyType() == DB::KVDB::KeyType::INTEGER) {
			std::vector<DB::DB_BASE::KVPairInt> kvs = workload_.NextTransactionKVsInt();
			return kv_db_->Insert(kvs);
		} else {
			return kv_db_->Insert();
		}
	} else {
		assert(db_ != NULL);
		const std::string &table = workload_.NextTable();
		const std::string &key = workload_.NextSequenceKey();
		std::vector<DB::DB_BASE::KVPair> values;
		workload_.BuildValues(values);
		return db_->Insert(table, key, values);
	}
}

} // Ycsb

#endif // YCSB_C_CLIENT_H_
