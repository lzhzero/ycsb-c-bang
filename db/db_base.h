/*
 * Author: Ning Gao(nigo9731@colorado.edu)
 *
 * DB_BASE is the parent all database classes. It's further divided into KVDB and TableDB
 */

#ifndef DTRANX_DB_BASE_H_
#define DTRANX_DB_BASE_H_

#include <iostream>
namespace ycsbc {

class DB_BASE {
public:
	typedef std::pair<std::string, std::string> KVPair;
	static const int kOK = 0;
	static const int kErrorNoData = 1;
	static const int kErrorConflict = 2;
	virtual ~DB_BASE(){
	}
};
}

#endif /* DTRANX_DB_BASE_H_ */
