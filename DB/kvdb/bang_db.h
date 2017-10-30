/*
 * Author: Ning Gao(nigo9731@colorado.edu)
 *
 * Hyperdex DB client Handler
 *  connect to the coordinator not the other slave nodes
 *
 * default table has one attribute called "value"
 * space:
 * key: {"value": "valuestr"}
 */

#ifndef DTRANX_BANG_DB_H_
#define DTRANX_BANG_DB_H_

//#include <hyperdex/client.h>
#include <bangdb-client/database.h>
#include "DB/commons.h"
namespace Ycsb {
namespace DB {

static std::string SPACE = "ning";
static std::string ATTR = "value";

class BangDB: public KVDB {
public:
    BangDB()
            : client_() {
        shareDB = false;
        keyType = KeyType::STRING;
    }

    BangDB(const BangDB& other) {
        std::cout << "BangDB copy contructor is called" << std::endl;
        ips_ = other.ips_;
        shareDB = other.shareDB;
        keyType = other.keyType;
        client_ = NULL;
    }

    ~BangDB() {
        Close();
    }

    KVDB* Clone(int index) {
        BangDB *instance = new BangDB(*this);
        std::cout << "Cloning BangDB called" << std::endl;
        instance->client_ = hyperdex_client_create(ips_[0].c_str(), HYPERDEX_SERVER_PORT);
        return instance;
    }

    void Init(std::vector<std::string> ips, std::string selfAddress, int localStartPort,
            bool fristTime) {
        ips_ = ips;
        assert(ips_.size() > 0);
        client_ = hyperdex_client_create(ips[0].c_str(), HYPERDEX_SERVER_PORT);
    }

    void Close() {
        if (client_) {
            hyperdex_client_destroy(client_);
        }
    }

    int Read(std::vector<std::string> keys) {
        enum hyperdex_client_returncode tranx_status;
        enum hyperdex_client_returncode loop_status;
        int64_t code = -1;
        int64_t loopCode = -1;
        int triedTimes = 0;
        struct hyperdex_client_transaction* tranx = hyperdex_client_begin_transaction(client_);
        struct hyperdex_client_attribute* attrs;
        size_t attrs_sz = 0;
        for (auto it = keys.begin(); it != keys.end(); ++it) {
            tranx_status = HYPERDEX_CLIENT_NOTFOUND;
            attrs_sz = 0;
            code = hyperdex_client_xact_get(tranx, SPACE.c_str(), it->c_str(), it->size(),
                    &tranx_status, (const hyperdex_client_attribute**) &attrs, &attrs_sz);
            triedTimes = 0;
            while (loopCode != code) {
                loopCode = hyperdex_client_loop(client_, 0, &loop_status);
                triedTimes++;
                if (triedTimes == 60) {
                    break;
                }
                usleep(50);
            }
            if (tranx_status != HYPERDEX_CLIENT_SUCCESS) {
                /*
                 std::cout << "reading " << (*it) << " failure "
                 << hyperdex_client_returncode_to_string(tranx_status)
                 << " "
                 << hyperdex_client_returncode_to_string(loop_status)
                 << std::endl;
                 */
                hyperdex_client_abort_transaction(tranx);
                return kErrorNoData;
            }
            if (attrs_sz != 0) {
                hyperdex_client_destroy_attrs(attrs, attrs_sz);
            }
        }
        tranx_status = HYPERDEX_CLIENT_ABORTED;
        code = hyperdex_client_commit_transaction(tranx, &tranx_status);
        triedTimes = 0;
        while (loopCode != code) {
            loopCode = hyperdex_client_loop(client_, 0, &loop_status);
            triedTimes++;
            if (triedTimes == 60) {
                break;
            }
            usleep(50);
        }
        if (tranx_status == HYPERDEX_CLIENT_COMMITTED || tranx_status == HYPERDEX_CLIENT_SUCCESS) {
            return kOK;
        } else {
            return kErrorConflict;
        }
    }

    int ReadSnapshot(std::vector<std::string> keys) {
        /*
         * read snapshot is not supported by hyperdex yet
         */
        return kOK;
    }

    int Update(std::vector<KVPair> writes) {
        enum hyperdex_client_returncode tranx_status;
        enum hyperdex_client_returncode loop_status;
        int64_t code = -1;
        int64_t loopCode = -1;
        int triedTimes = 0;
        struct hyperdex_client_transaction* tranx = hyperdex_client_begin_transaction(client_);

        enum hyperdex_client_returncode op_status;
        struct hyperdex_client_attribute attr;

        for (auto it = writes.begin(); it != writes.end(); ++it) {
            code = hyperdex_client_xact_put(tranx, SPACE.c_str(), it->first.c_str(),
                    it->first.size(), &attr, 1, &op_status);
            triedTimes = 0;
            while (loopCode != code) {
                loopCode = hyperdex_client_loop(client_, 0, &loop_status);
                triedTimes++;
                if (triedTimes == 60) {
                    break;
                }
                usleep(50);
            }
        }
        code = hyperdex_client_commit_transaction(tranx, &tranx_status);
        triedTimes = 0;
        while (loopCode != code) {
            loopCode = hyperdex_client_loop(client_, 0, &loop_status);
            triedTimes++;
            if (triedTimes == 60) {
                break;
            }
            usleep(50);
        }

        if (tranx_status == HYPERDEX_CLIENT_COMMITTED || tranx_status == HYPERDEX_CLIENT_SUCCESS) {
            return kOK;
        } else {
            return kErrorConflict;
        }
    }

    int ReadWrite(std::vector<std::string> reads, std::vector<KVPair> writes) {
        enum hyperdex_client_returncode tranx_status;
        enum hyperdex_client_returncode loop_status;
        enum hyperdex_client_returncode op_status;
        int64_t code = -1;
        int64_t loopCode = -1;
        int triedTimes = 0;
        struct hyperdex_client_transaction* tranx = hyperdex_client_begin_transaction(client_);

        struct hyperdex_client_attribute attr;
        struct hyperdex_client_attribute* attrs;
        size_t attrs_sz = 0;

        for (auto it = reads.begin(); it != reads.end(); ++it) {
            tranx_status = HYPERDEX_CLIENT_NOTFOUND;
            attrs_sz = 0;
            code = hyperdex_client_xact_get(tranx, SPACE.c_str(), it->c_str(), it->size(),
                    &tranx_status, (const hyperdex_client_attribute**) &attrs, &attrs_sz);
            triedTimes = 0;
            while (loopCode != code) {
                loopCode = hyperdex_client_loop(client_, 0, &loop_status);
                triedTimes++;
                if (triedTimes == 60) {
                    break;
                }
                usleep(50);
            }
            if (tranx_status != HYPERDEX_CLIENT_SUCCESS) {
                //std::cout << "reading " << (*it) << " failure" << std::endl;
                hyperdex_client_abort_transaction(tranx);
                return kErrorNoData;
            }
            if (attrs_sz != 0) {
                hyperdex_client_destroy_attrs(attrs, attrs_sz);
            }
        }
        for (auto it = writes.begin(); it != writes.end(); ++it) {
            attr.attr = ATTR.c_str();
            attr.value = it->second.c_str();
            attr.datatype = hyperdatatype::HYPERDATATYPE_STRING;
            attr.value_sz = it->second.size();
            op_status = HYPERDEX_CLIENT_NOTFOUND;
            code = hyperdex_client_xact_put(tranx, SPACE.c_str(), it->first.c_str(),
                    it->first.size(), &attr, 1, &op_status);
            triedTimes = 0;
            while (loopCode != code) {
                loopCode = hyperdex_client_loop(client_, 0, &loop_status);
                triedTimes++;
                if (triedTimes == 60) {
                    break;
                }
                usleep(50);
            }
            if (op_status != HYPERDEX_CLIENT_SUCCESS) {
                /*
                 std::cout << "writing " << (it->first) << " failure"
                 << std::endl;
                 */
                hyperdex_client_abort_transaction(tranx);
                return kErrorNoData;
            }
        }
        tranx_status = HYPERDEX_CLIENT_ABORTED;
        code = hyperdex_client_commit_transaction(tranx, &tranx_status);
        triedTimes = 0;
        while (loopCode != code) {
            loopCode = hyperdex_client_loop(client_, 0, &loop_status);
            triedTimes++;
            if (triedTimes == 60) {
                break;
            }
            usleep(50);
        }

        if (tranx_status == HYPERDEX_CLIENT_COMMITTED || tranx_status == HYPERDEX_CLIENT_SUCCESS) {
            return kOK;
        } else {
            return kErrorConflict;
        }
    }

    int Insert(std::vector<KVPair> writes) {
        enum hyperdex_client_returncode tranx_status;
        enum hyperdex_client_returncode loop_status;
        int64_t code = -1;
        int64_t loopCode = -1;
        int triedTimes = 0;
        struct hyperdex_client_transaction* tranx = hyperdex_client_begin_transaction(client_);

        enum hyperdex_client_returncode op_status;
        struct hyperdex_client_attribute attr;

        for (auto it = writes.begin(); it != writes.end(); ++it) {
            code = hyperdex_client_xact_put(tranx, SPACE.c_str(), it->first.c_str(),
                    it->first.size(), &attr, 1, &op_status);
            triedTimes = 0;
            while (loopCode != code) {
                loopCode = hyperdex_client_loop(client_, 0, &loop_status);
                triedTimes++;
                if (triedTimes == 60) {
                    break;
                }
                usleep(50);
            }
        }
        code = hyperdex_client_commit_transaction(tranx, &tranx_status);
        triedTimes = 0;
        while (loopCode != code) {
            loopCode = hyperdex_client_loop(client_, 0, &loop_status);
            triedTimes++;
            if (triedTimes == 60) {
                break;
            }
            usleep(50);
        }

        if (tranx_status == HYPERDEX_CLIENT_COMMITTED || tranx_status == HYPERDEX_CLIENT_SUCCESS) {
            return kOK;
        } else {
            return kErrorConflict;
        }
    }
private:
    struct hyperdex_client* client_;
    std::vector<std::string> ips_;
};
} // DB
} // Ycsb

#endif /* DTRANX_BANG_DB_H_ */

