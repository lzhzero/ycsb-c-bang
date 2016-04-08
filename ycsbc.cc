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
#include <vector>
#include <future>
#include "core/utils.h"
#include "core/timer.h"
#include "core/client.h"
#include "core/core_workload.h"
#include "db/db_factory.h"

using namespace std;

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);

/*
 * Thread function
 * for DTranx(key value store), DB is shared among threads while DTranx Clients are shared.
 */
int DelegateClient(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops,
bool is_loading, std::vector<std::string> ips, std::vector<DTranx::Client::Client*> clients, int clientID) {
	//clientID controls whether to store workload or not for analysis
	ycsbc::DtranxDB *dtranx_db = NULL;
	if (clients.size() > 0) {
		dtranx_db = new ycsbc::DtranxDB();
	}
	if (db) {
		db->Init();
	}
	if (dtranx_db) {
		dtranx_db->Init(ips, clients);
	}
	ycsbc::Client client(db, dtranx_db, *wl, clientID);
	int oks = 0;
	for (int i = 0; i < num_ops; ++i) {
		if (is_loading) {
			oks += client.DoInsert();
		} else {
			oks += client.DoTransaction();
		}
	}
	if (db) {
		db->Close();
	}
	if (dtranx_db) {
		dtranx_db->Close();
	}
	return oks;
}

int main(const int argc, const char *argv[]) {
	utils::Properties props;
	string file_name = ParseCommandLine(argc, argv, props);
	std::string clusterFileName = props.GetProperty("clusterfilename", "");

	//create shared clients for dtranx
	std::vector<DTranx::Client::Client*> clients;
	std::vector<std::string> ips;
	if (props["dbname"] == "dtranx") {
		DTranx::Util::ConfigHelper configHelper;
		configHelper.readFile("DTranx.conf");
		if (clusterFileName.empty()) {
			ips.push_back(configHelper.read("SelfAddress"));
		} else {
			std::ifstream ipFile(clusterFileName);
			if (!ipFile.is_open()) {
				cout << "cannot open " << clusterFileName << endl;
				exit(0);
			}
			std::string ip;
			while (std::getline(ipFile, ip)) {
				ips.push_back(ip);
			}
		}
		std::shared_ptr<zmq::context_t> context = std::make_shared<
						zmq::context_t>(
						configHelper.read<DTranx::uint32>("maxIOThreads"));
		for(auto it= ips.begin(); it!= ips.end(); ++it){
			clients.push_back(new DTranx::Client::Client(*it, configHelper.read < std::string > ("routerFrontPort"), context));
		}
	}

	ycsbc::DB *db = ycsbc::DBFactory::CreateDB(props["dbname"]);
	ycsbc::CoreWorkload wl;
	wl.Init(props);
	if(props.GetProperty("genwork", "0") == "1"){
		std::ofstream workFile("genwork");
		if (!workFile.is_open()) {
			cout << "cannot open " << "genwork" << endl;
			exit(0);
		}
		int total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
		for(int i=0; i< total_ops; i++){
			std::string key = wl.NextTransactionKey();
			workFile << key<< endl;
		}
		exit(0);
	}

	const int num_threads = stoi(props.GetProperty("threadcount", "1"));

	// Loads data
	vector<future<int>> actual_ops;
	int total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
	for (int i = 0; i < num_threads; ++i) {
		actual_ops.emplace_back(
				async(launch::async, DelegateClient, db, &wl,
						total_ops / num_threads, true, ips, clients, -1));
	}
	assert((int )actual_ops.size() == num_threads);

	int sum = 0;
	for (auto &n : actual_ops) {
		assert(n.valid());
		sum += n.get();
	}
	cerr << "# Loading records:\t" << sum << endl;

	// Peforms transactions
	actual_ops.clear();
	total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
	utils::Timer<double> timer;
	timer.Start();
	for (int i = 0; i < num_threads; ++i) {
		actual_ops.emplace_back(
				async(launch::async, DelegateClient, db, &wl,
						total_ops / num_threads, false, ips, clients, -1));
	}
	assert((int )actual_ops.size() == num_threads);
	total_ops = total_ops / num_threads * num_threads;

	sum = 0;
	for (auto &n : actual_ops) {
		assert(n.valid());
		sum += n.get();
	}
	double duration = timer.End();
	cerr << "# Transaction throughput (KTPS)" << endl;
	cerr << props["dbname"] << '\t' << file_name << '\t' << num_threads << '\t';
	cerr << sum / duration / 1000 << endl;
	cerr <<"total_ops: "<< total_ops<<", success: "<<sum<<", percentage: "<<1.0* sum/total_ops<< endl;

	/*
	for(auto it=clients.begin(); it != clients.end(); ++it){
		delete it->second;
	}
	*/
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
		}  else if (strcmp(argv[argindex], "-genwork") == 0) {
			argindex++;
			if (argindex >= argc) {
				UsageMessage(argv[0]);
				exit(0);
			}
			props.SetProperty("genwork", argv[argindex]);
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
	cout << "  -genwork n: if n equals 1, generate keys to a file named genwork.data (default: 0)" << endl;
	cout << "  -threads n: execute using n threads (default: 1)" << endl;
	cout << "  -db dbname: specify the name of the DB to use (default: basic)"
			<< endl;
	cout
			<< "  -P propertyfile: load properties from the given file. Multiple files can"
			<< endl;
	cout
			<< "  -C clusterfile: only used for dtranx db, load ip addresses from the given file."
			<< endl;
	cout
			<< "                   be specified, and will be processed in the order specified"
			<< endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
	return strncmp(str, pre, strlen(pre)) == 0;
}

