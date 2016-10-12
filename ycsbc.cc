//
//  ycsbc.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <cstring>
#include <string>
#include <iostream>
#include <ctime>
#include <vector>
#include <future>
#include <chrono>
#include "core/utils.h"
#include "core/timer.h"
#include "core/client.h"
#include "core/core_workload.h"
#include "db/db_factory.h"
#include "DTranx/Util/Log.h"

using namespace std;
//aggregate throughput during the previous 20 seconds
int ONLINE_THROUGHPUT_TIME = 1000000;

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);

struct DBData {
	bool isKV;
	std::vector<std::string> ips;
	ycsbc::DB *db;
	ycsbc::KVDB *kvdb;
	std::string dbName;
	DBData() :
			isKV(false), ips(), db(NULL), kvdb(NULL) {
	}
};

/*
 * Thread function
 * for DTranx(key value store), DB is independently instantiated among threads while DTranx Clients are shared.
 */
void DelegateClient(DBData *dbData, ycsbc::CoreWorkload *wl, const int num_ops,
bool is_loading, std::atomic<int> *sum) {
	/*
	 * when testing hyperdexDB, create a kvdb instance for each thread
	 * because each thread needs one socket connection to reach the max throughput
	 */
	//ycsbc::KVDB* kvdb = dynamic_cast<ycsbc::KVDB*>(ycsbc::DBFactory::CreateDB("hyperdex"));
	//kvdb->Init(dbData->ips);
	if (dbData->isKV) {
		assert(dbData->kvdb != NULL);
	} else {
		assert(dbData->db != NULL);
		dbData->db->Init();
	}
	/*
	 * btreedb is created for each thread to avoid metadata reading/writing
	 */
	if (dbData->isKV && !dbData->kvdb->isDBShared()) {
		ycsbc::KVDB *kvdb = dbData->kvdb->Clone();
		ycsbc::Client client(dbData->db, kvdb, *wl);
		for (int i = 0; i < num_ops; ++i) {
			if (is_loading) {
				if (client.DoInsert()) {
					sum->operator ++();
				}
			} else {
				if (client.DoTransaction()) {
					sum->operator ++();
				}
			}
		}
		delete kvdb;
		return;
	}

	ycsbc::Client client(dbData->db, dbData->kvdb, *wl);
	for (int i = 0; i < num_ops; ++i) {
		if (is_loading) {
			if (client.DoInsert()) {
				sum->operator ++();
			}
		} else {
			if (client.DoTransaction()) {
				sum->operator ++();
			}
		}
	}
	//kvdb->Close();
	//delete kvdb;
}

int main(const int argc, const char *argv[]) {

	 DTranx::Util::Log::setLogPolicy( { { "Client", "PROFILE" }, { "Server", "PROFILE" }, { "Tranx",
	 "PROFILE" }, { "Storage", "PROFILE" }, { "RPC", "PROFILE" }, {"Util", "PROFILE"}, { "Log", "PROFILE" } });

	utils::Properties props;
	string file_name = ParseCommandLine(argc, argv, props);

	/*
	 * prepare clients, ips etc. for dtranx
	 */
	DBData dbData;
	dbData.dbName = props["dbname"];
	if (props["dbname"] == "dtranx" || props["dbname"] == "hyperdex"
			|| props["dbname"] == "btree") {
		dbData.isKV = true;
		std::string clusterFileName = props.GetProperty("clusterfilename", "");
		if (clusterFileName.empty()) {
			cout << "please use -C for cluster filename" << endl;
			return 0;
		}
		std::ifstream ipFile(clusterFileName);
		if (!ipFile.is_open()) {
			cout << "cannot open " << clusterFileName << endl;
			exit(0);
		}
		std::string ip;
		while (std::getline(ipFile, ip)) {
			dbData.ips.push_back(ip);
		}
	}
	if (dbData.isKV) {
		dbData.kvdb = dynamic_cast<ycsbc::KVDB*>(ycsbc::DBFactory::CreateDB(
				props["dbname"]));
		dbData.kvdb->Init(dbData.ips, props["selfAddress"]);
	} else {
		dbData.db = dynamic_cast<ycsbc::DB*>(ycsbc::DBFactory::CreateDB(
				props["dbname"]));
	}
	ycsbc::CoreWorkload wl;
	wl.Init(props);

	/*
	 * generate a keys file if needed
	 */
	if (props.GetProperty("genwork", "0") == "1") {
		std::ofstream workFile("genwork");
		if (!workFile.is_open()) {
			cout << "cannot open " << "genwork" << endl;
			exit(0);
		}
		int total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
		for (int i = 0; i < total_ops; i++) {
			std::string key = wl.NextTransactionKey();
			workFile << key << endl;
		}
		exit(0);
	}

	const int num_threads = stoi(props.GetProperty("threadcount", "1"));
	/*
	 *  Loads data
	 */
	vector<std::thread> threads;
	int total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
	//int sums[num_threads];
	std::atomic<int> sums[num_threads];
	for (int i = 0; i < num_threads; ++i) {
		sums[i].store(0);
	}
	int sum = 0;
	if (total_ops >= num_threads) {
		for (int i = 0; i < num_threads; ++i) {
			threads.push_back(
					std::thread(DelegateClient, &dbData, &wl,
							total_ops / num_threads,
							true, &sums[i]));
		}
		sum = 0;
		std::chrono::system_clock::time_point start =
				std::chrono::system_clock::now();
		std::chrono::system_clock::time_point end;
		while (sum < total_ops * 0.8) {
			sum = 0;
			for (int i = 0; i < num_threads; ++i) {
				sum += sums[i].load();
			}
			time_t t = time(0);   // get time now
			struct tm * now = localtime(&t);
			cout << now->tm_sec << ": " << sum << endl;
			sleep(1);
			end = std::chrono::system_clock::now();
			if (std::chrono::duration_cast<std::chrono::seconds>(end - start).count()
					> ONLINE_THROUGHPUT_TIME) {
				break;
			}

		}
		sum = 0;

		for (int i = 0; i < num_threads; ++i) {
			threads[i].join();
		}
		threads.clear();
	}
	sum = 0;
	for (int i = 0; i < num_threads; ++i) {
		sum += sums[i].load();
	}
	cerr << "# Loading records:\t" << sum << endl;

	for (int i = 0; i < num_threads; ++i) {
		sums[i].store(0);
	}

	/*
	 * Performs transactions
	 */
	total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
	utils::Timer<double> timer;
	timer.Start();
	for (int i = 0; i < num_threads; ++i) {
		threads.push_back(
				std::thread(DelegateClient, &dbData, &wl,
						total_ops / num_threads,
						false, &sums[i]));
	}
	sum = 0;
	std::chrono::system_clock::time_point start =
			std::chrono::system_clock::now();
	std::chrono::system_clock::time_point end;
	while (sum < total_ops * 0.8) {
		sum = 0;
		for (int i = 0; i < num_threads; ++i) {
			sum += sums[i].load();
		}
		time_t t = time(0);   // get time now
		struct tm * now = localtime(&t);
		cout << now->tm_sec << ": " << sum << endl;
		sleep(1);
		end = std::chrono::system_clock::now();
		if (std::chrono::duration_cast<std::chrono::seconds>(end - start).count()
				> ONLINE_THROUGHPUT_TIME) {
			break;
		}
	}
	sum = 0;
	for (int i = 0; i < num_threads; ++i) {
		threads[i].join();
	}
	threads.clear();
	total_ops = total_ops / num_threads * num_threads;

	for (int i = 0; i < num_threads; ++i) {
		sum += sums[i].load();
	}
	double duration = timer.End();
	cerr << "# Transaction throughput (KTPS)" << endl;
	cerr << props["dbname"] << '\t' << file_name << '\t' << num_threads << '\t';
	cerr << sum / duration / 1000 << endl;
	cerr << "total_ops: " << total_ops << ", success: " << sum
			<< ", percentage: " << 1.0 * sum / total_ops << endl;
	if (dbData.isKV) {
		dbData.kvdb->Close();
	}
}

string ParseCommandLine(int argc, const char *argv[],
		utils::Properties &props) {
	int argindex = 1;
	string filename;
	while (argindex < argc && StrStartWith(argv[argindex], "-")) {
		if (strcmp(argv[argindex], "-threads") == 0) {
			argindex++;
			if (argindex >= argc) {
				UsageMessage(argv[0]);
				exit(0);
			}
			props.SetProperty("threadcount", argv[argindex]);
			argindex++;
		} else if (strcmp(argv[argindex], "-genwork") == 0) {
			argindex++;
			if (argindex >= argc) {
				UsageMessage(argv[0]);
				exit(0);
			}
			props.SetProperty("genwork", argv[argindex]);
			argindex++;
		} else if (strcmp(argv[argindex], "-s") == 0) {
			argindex++;
			if (argindex >= argc) {
				UsageMessage(argv[0]);
				exit(0);
			}
			props.SetProperty("selfAddress", argv[argindex]);
			argindex++;
		} else if (strcmp(argv[argindex], "-db") == 0) {
			argindex++;
			if (argindex >= argc) {
				UsageMessage(argv[0]);
				exit(0);
			}
			props.SetProperty("dbname", argv[argindex]);
			argindex++;
		} else if (strcmp(argv[argindex], "-C") == 0) {
			argindex++;
			if (argindex >= argc) {
				UsageMessage(argv[0]);
				exit(0);
			}
			props.SetProperty("clusterfilename", argv[argindex]);
			argindex++;
		} else if (strcmp(argv[argindex], "-P") == 0) {
			argindex++;
			if (argindex >= argc) {
				UsageMessage(argv[0]);
				exit(0);
			}
			filename.assign(argv[argindex]);
			ifstream input(argv[argindex]);
			try {
				props.Load(input);
			} catch (const string &message) {
				cout << message << endl;
				exit(0);
			}
			input.close();
			argindex++;
		} else {
			cout << "Unknown option " << argv[argindex] << endl;
			exit(0);
		}
	}

	if (argindex == 1 || argindex != argc) {
		UsageMessage(argv[0]);
		exit(0);
	}

	return filename;
}

void UsageMessage(const char *command) {
	cout << "Usage: " << command << " [options]" << endl;
	cout << "Options:" << endl;
	cout
			<< "  -genwork n: if n equals 1, generate keys to a file named genwork.data (default: 0)"
			<< endl;
	cout << "  -threads n: execute using n threads (default: 1)" << endl;
	cout << "  -db dbname: specify the name of the DB to use (default: basic)"
			<< endl;
	cout
			<< "  -P propertyfile: load properties from the given file. Multiple files can"
			<< endl;
	cout
			<< "  -C clusterfile: only used for dtranx db, load ip addresses from the given file."
			<< endl;
	cout << "  -s selfAddress: self address that clients bind to." << endl;
	cout
			<< "                   be specified, and will be processed in the order specified"
			<< endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
	return strncmp(str, pre, strlen(pre)) == 0;
}

