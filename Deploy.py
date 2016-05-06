#!/usr/bin/env python

import os
import paramiko
import argparse
import subprocess

#Constants
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
	scp(True, LibPath + "libproto*", username, server, RemoteDir)
	scp(True, LibPath + "libzmq*", username, server, RemoteDir)
	scp(True, "/usr/lib/libcrypto*", username, server, RemoteDir)
	scp(True, "/usr/lib/libtbb*", username, server, RemoteDir)
	runSSHCommand(server, username, "sudo mv "+RemoteDir+ "lib* /usr/lib/")

def runBash(command):
	out, err = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True).communicate()
	return out

parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers()
runtime_parser = subparsers.add_parser('prepare', help='deployment for runtime environment')
runtime_parser.set_defaults(subcommand='prepare')
runtime_parser.add_argument("-i","--ip",dest="ip",help="ip address", nargs =1)

args = parser.parse_args()

if args.subcommand == 'prepare':
	print "prepare for ycsb test"
	ip = args.ip[0]
	runSSHCommand(ip, RemoteUser, "mkdir ~/Test; sudo chmod -R a+rwx ~/Test")
	scpLibrary(ip, RemoteUser)
	dtranxBash="\
	scp ycsbc "+RemoteUser+"@"+ip+":" + RemoteDir+";\
	scp ../DTranx/ips "+RemoteUser+"@"+ip+":" + RemoteDir+";\
	scp workloads/workloada.spec "+RemoteUser+"@"+ip+":" + RemoteDir+";"
	runBash(dtranxBash)	
