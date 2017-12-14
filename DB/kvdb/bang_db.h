/*
 * Author: Ning Gao(nigo9731@colorado.edu)
 *         Zhang Liu(zhli5318@colorado.edu)
 *
 * BangDB client Handler
 *  connect to the coordinator not the other slave nodes
 *
 * NOTE: server IP and port will be loaded from bangdb.config during runtime
 *
 * NOTE:client_ is databace class in bangdb_client Namespace
 *
 * key: {"value": "valuestr"}
 */

#ifndef DTRANX_BANG_DB_H_
#define DTRANX_BANG_DB_H_

#include <bangdb-client/database.h>
#include "kvdb.h"

#include "DB/commons.h"
namespace Ycsb {
namespace DB {


class BangDB: public KVDB {
public:
    BangDB()
            : client_() {
        shareDB = false;
        keyType = KeyType::STRING;
        tbl_ = NULL;
    }

    BangDB(const BangDB& other) {
        std::cout << "BangDB copy contructor is called" << std::endl;
        shareDB = other.shareDB;
        keyType = other.keyType;
        client_ = NULL;
        tbl_ = NULL;
    }

    ~BangDB() {
        Close();
    }

    KVDB* Clone(int index) {
        BangDB *instance = new BangDB(*this);
        std::cout << "Cloning BangDB called" << std::endl;
        instance->client_ = new bangdb_client::database((char*)"mydb", NULL, bangdb_client::DB_OPTIMISTIC_TRANSACTION, NULL, NULL);
        bangdb_client::table_env tenv;

        //configure table properties
        //tenv.set_key_size_byte();
        tenv.set_persist_type(bangdb_client::PERSIST_ONLY);
        tenv.set_autocommit_state(1);

        if((instance->tbl_ = client_->gettable((char*)"mytbl", bangdb_client::OPENCREATE, &tenv)) == NULL)
        {
            printf("table NULL error, quitting");
            exit(-1);
        }
        return instance;
    }

    void Init(std::vector<std::string> ips, std::string selfAddress, int localStartPort,
            bool fristTime) {
        client_ = new bangdb_client::database((char*)"mydb", NULL, bangdb_client::DB_OPTIMISTIC_TRANSACTION, NULL, NULL);
        bangdb_client::table_env tenv;

        //configure table properties
        //tenv.set_key_size_byte();
        tenv.set_persist_type(bangdb_client::PERSIST_ONLY);
        tenv.set_autocommit_state(true);

        if((tbl_ = client_->gettable((char*)"mytbl", bangdb_client::OPENCREATE, &tenv)) == NULL)
        	{
        		printf("table NULL error, quitting");
        		exit(-1);
        	}
    }

    void Close() {
	if (client_) {
            client_->closedatabase();
        }
    }

    int Read(std::vector<std::string> keys) {
    	bangdb_client::connection *conn = tbl_->getconnection();
    	if(!conn)
    	{
    		printf("connection null error, please change the max conn config in bangdb.config, it could be the reason\n");
    		return -1;
    	}

    	bangdb_client::bangdb_txn* txn;
    	client_->begin_transaction(txn);

        for (auto it = keys.begin(); it != keys.end(); ++it) {

        		bangdb_client::FDT ikey, *ival = NULL;
			//key = it->c_str();
			//lens = it->size();
        		//ikey.data = key;
			//ikey.length = lens;
			ikey.data = (char*)it->c_str();
        		ikey.length = it->size();

        		if((ival = conn->get(&ikey, txn)) == NULL) {
        	    		client_->abort_transaction(txn);
        			printf("individual get transaction failed ");
        	    		return kErrorNoData;
        	    	}
        }
        if (client_->commit_transaction(txn) >= 0) {
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

		bangdb_client::connection *conn = tbl_->getconnection();
		if(!conn)
		{
			printf("connection null error, please change the max conn config in bangdb.config, it could be the reason\n");
			return -1;
		}

		bangdb_client::bangdb_txn* txn;
		client_->begin_transaction(txn);

		for (auto it = writes.begin(); it != writes.end(); ++it) {
    			bangdb_client::FDT ikey, ival;
    			ikey.data = (char*)it->first.c_str();
    			ikey.length = it->first.size();
    			ival.data = (char*)it->second.c_str();
    			ival.length = it->second.size();
    			if(conn->put(&ikey, &ival, bangdb_client::INSERT_UNIQUE, txn) < 0) {
    				printf("individual update transaction failed ");
    				exit(1);

    			}
		}

		if (client_->commit_transaction(txn) >= 0) {
			return kOK;
		} else {
			return kErrorConflict;
		}
    }

    int ReadWrite(std::vector<std::string> reads, std::vector<KVPair> writes) {

    		bangdb_client::connection *conn = tbl_->getconnection();
    		if(!conn) {
    			printf("connection null error, please change the max conn config in bangdb.config, it could be the reason\n");
    			return -1;
    		}

    		bangdb_client::bangdb_txn* txn;
    		client_->begin_transaction(txn);

    		for (auto it = reads.begin(); it != reads.end(); ++it) {
    			bangdb_client::FDT ikey, *ival = NULL;
    			ikey.data = (char*)it->c_str();
    			ikey.length = it->size();

       		if((ival = conn->get(&ikey, txn)) == NULL) {
        	    		printf("individual read transaction failed ");
        	    		return kErrorNoData;
        	    	}
        }
        for (auto it = writes.begin(); it != writes.end(); ++it) {
        		bangdb_client::FDT ikey, ival;
        	    ikey.data = (char*)it->first.c_str();
        	    ikey.length = it->first.size();
        	    ival.data = (char*)it->second.c_str();
        	    ival.length = it->second.size();
        	    if(conn->put(&ikey, &ival, bangdb_client::INSERT_UNIQUE, txn) < 0) {
        	        printf("individual write transaction failed ");
        	        	exit(1);

        	    }
        }
        if (client_->commit_transaction(txn) >= 0) {
        		return kOK;
        	} else {
        		return kErrorConflict;
        	}
    }

    int Insert(std::vector<KVPair> writes) {

    		bangdb_client::connection *conn = tbl_->getconnection();
    		if(!conn)
    		{
    			printf("connection null error, please change the max conn config in bangdb.config, it could be the reason\n");
    			return -1;
    		}

    		bangdb_client::bangdb_txn* txn;
        client_->begin_transaction(txn);

        for (auto it = writes.begin(); it != writes.end(); ++it) {
        		bangdb_client::FDT ikey, ival;
        		ikey.data = (char*)it->first.c_str();
        		ikey.length = it->first.size();
        		ival.data = (char*)it->second.c_str();
        		ival.length = it->second.size();
        		if(conn->put(&ikey, &ival, bangdb_client::INSERT_UNIQUE, txn) < 0) {
        			printf("individual insert transaction failed ");
        			exit(1);

        		}
        }

        if (client_->commit_transaction(txn) >= 0) {
        		return kOK;
        } else {
        		return kErrorConflict;
        }
    }

private:
    bangdb_client::database *client_;
    bangdb_client::table* tbl_;
};
} // DB
} // Ycsb

#endif /* DTRANX_BANG_DB_H_ */

