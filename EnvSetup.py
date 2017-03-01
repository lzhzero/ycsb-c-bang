#!/usr/bin/env python

import os
import paramiko
import argparse
import subprocess
import time
import threading

#Constants
RemoteHomeDir = "/users/nigo9731/"
RemoteDir = "/users/nigo9731/Test/"
RemoteUser = "nigo9731"
LibPath = "/usr/local/lib/"

def scp(direction, localFile, user, server, path):
	if direction == True:
		os.system("scp " + localFile + " " + user + "@" + server + ":" + path)
	else:
		os.system("scp " + user + "@" + server + ":" + path + " " + localFile)

def runSSHCommand(server, username, command):
	client = paramiko.client.SSHClient()
	client.load_system_host_keys()
	client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	client.connect(server, username=username)
	stdin, stdout, stderr = client.exec_command(command)
	stdoutAsString = []
	stderrAsString = []
	for line in stdout:
		stdoutAsString.append(line)
	for line in stderr:
		stderrAsString.append(line)
	return stdoutAsString, stderrAsString

def scpLibrary(server, username):
	scp(True, "/usr/lib/x86_64-linux-gnu/libtbb*", username, server, RemoteHomeDir)
	scp(True, LibPath + "libbtree*", username, server, RemoteHomeDir)
	scp(True, LibPath + "librtree*", username, server, RemoteHomeDir)
	scp(True, LibPath + "libdtranx*", username, server, RemoteHomeDir)
	runSSHCommand(server, username, "sudo mv "+RemoteHomeDir+ "libtbb* /usr/lib/")
	runSSHCommand(server, username, "sudo mv "+RemoteHomeDir+ "libbtree* /usr/local/lib/")
	runSSHCommand(server, username, "sudo mv "+RemoteHomeDir+ "librtree* /usr/local/lib/")
	runSSHCommand(server, username, "sudo mv "+RemoteHomeDir+ "libdtranx* /usr/local/lib/")

def runBash(command):
	out, err = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True).communicate()
	return out

def WaitForThreads(threads):
	while True:
		deleted_threads = []
		for thread in threads:
			if thread.isAlive() == False:
				thread.join()
				deleted_threads.append(thread)
		for thread in deleted_threads:
			threads.remove(thread)
		if len(threads) == 0:
			break;
		time.sleep(5)

parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers()
runtime_parser = subparsers.add_parser('run', help='deployment for runtime environment')
runtime_parser.set_defaults(subcommand='run')
runtime_parser.add_argument("-c","--cluster",dest="cluster",help="cluster config file required for vm mode: ip list", required = True)
runtime_parser.add_argument("-j","--job",dest="job",help=
	"job type: \n"
	"lib(install libraries);\n"
	"hyperdex(install hyperdex);\n"
	"bench(copy benchmark related files);\n"
 	"helper(run a command in each node)", required = True)

args = parser.parse_args()

if args.subcommand == 'run':
	ips = []
	f = open(args.cluster,'r')
	for line in f:
		ips.append(line.strip())
	print ips
	if args.job == "lib":
		#install dynamic library
		threads = []
		for ip in ips:
			print ip
			thread = threading.Thread(target = scpLibrary, args = (ip,RemoteUser,))
			thread.start()
			threads.append(thread)
		WaitForThreads(threads)
	elif args.job == "hyperdex":
		bash ="\
		echo deb [arch=amd64] http://ubuntu.hyperdex.org trusty main >> ./hyperdex.list;"
		runBash(bash) 
		threads = []
		for ip in ips:
			print ip
			thread = threading.Thread(target = RunHyperdex, args = (ip,RemoteUser,))
			thread.start()
			threads.append(thread)
		WaitForThreads(threads)
		runBash("rm ./hyperdex.list")
	elif args.job == "bench":
		baseIP = "192.168.0."
		index = 0
		localipsFile = open('localips', 'w')
		for ip in ips:
			index = index + 1
			localipsFile.write(baseIP + str(index) + '\n')
		localipsFile.close();
		for ip in ips:
			print ip
			print scp(True, "/home/neal/Documents/dev/YCSB-C-DTranx/ycsbc", RemoteUser, ip, "/users/nigo9731/")
			print scp(True, "/home/neal/Documents/dev/YCSB-C-DTranx/workloads/workloaddtranx.spec", RemoteUser, ip, "/users/nigo9731/")
			print scp(True, "/home/neal/Documents/dev/YCSB-C-DTranx/workloads/workloadbtree.spec", RemoteUser, ip, "/users/nigo9731/")
			print scp(True, "/home/neal/Documents/dev/YCSB-C-DTranx/workloads/workloadrtree.spec", RemoteUser, ip, "/users/nigo9731/")
			print scp(True, "/home/neal/Documents/dev/Scripts/metric/monitor.sh", RemoteUser, ip, "/users/nigo9731/")
			print scp(True, "/home/neal/Documents/dev/Scripts/metric/process.sh", RemoteUser, ip, "/users/nigo9731/")
			print scp(True, "localips", RemoteUser, ip, "/users/nigo9731/ips")
		runBash("rm localips");
