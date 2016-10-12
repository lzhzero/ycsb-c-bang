/*
 * Author: Ning Gao(nigo9731@colorado.edu)
 */

#ifndef YCSB_C_KVDB_H_
#define YCSB_C_KVDB_H_

#include <vector>
#include <string>
#include "db_base.h"

namespace ycsbc {

/*
 * KVDB means databases with clients shared among all threads
 */
class KVDB: public DB_BASE {
public:
	virtual void Init(std::vector<std::string> ips, std::string selfAddress) = 0;
	///
	/// Reads a record from the database.
	///
	/// @param keys are a list of read keys
	/// @return Zero on success, or a non-zero error code on error/record-miss.
	///
	virtual int Read(std::vector<std::string> keys) = 0;

	virtual int ReadSnapshot(std::vector<std::string> keys) = 0;
	///
	/// Updates a record in the database.
	/// Field/value pairs in the specified vector are written to the record,
	///
	/// @param reads, readset
	/// @param writes, writeset
	/// @return Zero on success, a non-zero error code on error.
	///
	virtual int Update(std::vector<std::string> reads,
			std::vector<KVPair> writes) = 0;

	///
	/// Inserts a record into the database.
	/// Field/value pairs in the specified vector are written into the record.
	///
	/// @param writes, a list of key value pair to insert.
	/// @return Zero on success, a non-zero error code on error.
	///
	virtual int Insert(std::vector<KVPair> writes) = 0;

	virtual int Write(std::vector<KVPair> writes) = 0;

	virtual ~KVDB() {
	}

	bool isDBShared(){
		return shareDB;
	}

	virtual void CreateDB(){

	}
	virtual void DestroyDB(){

	}
	virtual KVDB *Clone() = 0;
protected:
	/*
	 * shareDB means if we want to share DB instance among threads
	 */
	bool shareDB;
};

} // ycsbc

#endif // YCSB_C_KVDB_H_

