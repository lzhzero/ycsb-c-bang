//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "DB/db_factory.h"

#include "tabledb/basic_db.h"
#include "tabledb/lock_stl_db.h"
#include "tabledb/tbb_rand_db.h"
#include "tabledb/tbb_scan_db.h"
#include "kvdb/dtranx_db.h"
#include "kvdb/hyperdex_db.h"
#include "kvdb/btree_db.h"
#include "kvdb/rtree_db.h"

namespace Ycsb {
namespace DB {

DB_BASE* DBFactory::CreateDB(const std::string name) {
	if (name == "basic") {
		return new BasicDB;
	} else if (name == "lock_stl") {
		return new LockStlDB;
	} else if (name == "tbb_rand") {
		return new TbbRandDB;
	} else if (name == "tbb_scan") {
		return new TbbScanDB;
	} else if (name == "dtranx") {
		return new DtranxDB;
	} else if (name == "hyperdex") {
		return new HyperdexDB;
	} else if (name == "btree") {
		return new BtreeDB;
	} else if (name == "rtree") {
		return new RtreeDB;
	} else
		return NULL;
}

}
}
