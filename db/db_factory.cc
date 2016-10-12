//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "db/db_factory.h"

#include "db/basic_db.h"
#include "db/lock_stl_db.h"
#include "db/tbb_rand_db.h"
#include "db/tbb_scan_db.h"
#include "db/dtranx_db.h"
#include "db/hyperdex_db.h"
#include "db/btree_db.h"

using ycsbc::DB_BASE;
using ycsbc::DBFactory;

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
	} else
		return NULL;
}

