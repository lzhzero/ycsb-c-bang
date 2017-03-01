//
//  db.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/18/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_DB_FACTORY_H_
#define YCSB_C_DB_FACTORY_H_

#include "db_base.h"

namespace Ycsb {
namespace DB {

class DBFactory {
 public:
  static DB_BASE* CreateDB(const std::string name);
};
} // DB
} // Ycsb

#endif // YCSB_C_DB_FACTORY_H_

