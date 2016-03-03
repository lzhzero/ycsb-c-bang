/*
 * Author: Ning Gao(nigo9731@colorado.edu)
 */

#ifndef YCSB_C_DTRANX_DB_H_
#define YCSB_C_DTRANX_DB_H_

#include "core/db.h"

#include <mutex>
#include <iostream>
#include "DTranx/Client/ClientTranx.h"
#include "DTranx/Util/ConfigHelper.h"

namespace ycsbc {

using std::cout;
using std::endl;

class DtranxDB : public DB {
 public:
  void Init() {
    std::lock_guard<std::mutex> lock(mutex_);
    cout << "A new thread begins working." << endl;
    DTranx::Util::ConfigHelper configHelper;
    configHelper.readFile("DTranx.conf");
    std::shared_ptr<zmq::context_t> context = std::make_shared<zmq::context_t>(
    			configHelper.read<DTranx::uint32>("maxIOThreads"));
    clientTranx = new DTranx::Client::ClientTranx("DTranx.conf", context);
  }

  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string value;
    clientTranx->Read(const_cast<std::string&>(key), value);
    std::cout << "read key: " << key << " and the value is " << value << std::endl;
    bool success = clientTranx->Commit();
    if (success) {
    	cout << "commit success" << endl;
    } else {
    	cout << "commit failure" << endl;
    }
    clientTranx->Clear();
    return 0;
  }

  int Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result) {
    std::lock_guard<std::mutex> lock(mutex_);
    cout << "SCAN " << table << ' ' << key << " " << len;
    if (fields) {
      cout << " [ ";
      for (auto f : *fields) {
        cout << f << ' ';
      }
      cout << ']' << endl;
    } else {
      cout  << " < all fields >" << endl;
    }
    return 0;
  }

  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string value;
    clientTranx->Read(const_cast<std::string&>(key), value);
    std::cout << "update read key: " << key << " and the value is " << value << std::endl;
    clientTranx->Write(const_cast<std::string&>(key), values[0].second);
    cout << "update write key: " << key << " and the value is " << values[0].second << endl;
    bool success = clientTranx->Commit();
    if (success) {
        cout << "commit success" << endl;
    } else {
        cout << "commit failure" << endl;
    }
    clientTranx->Clear();
    return 0;
  }

  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {
    std::lock_guard<std::mutex> lock(mutex_);
    assert(values.size()!=0);
    clientTranx->Write(const_cast<std::string&>(key), values[0].second);
    cout << "write key: " << key << " and the value is " << values[0].second << endl;
    bool success = clientTranx->Commit();
    if (success) {
        cout << "commit success" << endl;
    } else {
        cout << "commit failure" << endl;
    }
    clientTranx->Clear();
    return 0;
  }

  int Delete(const std::string &table, const std::string &key) {
    std::lock_guard<std::mutex> lock(mutex_);
    cout << "DELETE " << table << ' ' << key << endl;
    return 0;
  }

 private:
  std::mutex mutex_;
  DTranx::Client::ClientTranx *clientTranx;
};

} // ycsbc


#endif /* YCSB_C_DTRANX_DB_H_ */
