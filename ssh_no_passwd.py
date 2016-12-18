#!/usr/bin/python
# -*- coding: utf-8 -*-
# *************************************************************
# ssh免密码登陆
# 配置文件: ssh_no_passwd.cfg
# 2016.09.22
# 曲怀觞
# 2016.10.01 更新
# 对当前用户配置ssh免密码登陆
# *************************************************************

# 使用DOM解析器
import xml.dom.minidom
import paramiko
import os
import time
import sys

# 检测参数数量
if ( len(sys.argv) - 1 ) != 2:
	sys.exit('Usage: ' + sys.argv[0] + ' passwd xml_file')

# 用户输入从节点信息:
# 用户名
#user = sys.argv[1]
user = (os.popen('whoami').read()).strip()
#print user + '!!!'
# 密码	
passwd = sys.argv[1]
# xml配置文件
xml_file = sys.argv[2]

# ssh端口
port = 22
# ssh证书存放目录
if user == 'root':
	ssh_path = '/root/.ssh/'
else:
	ssh_path = '/home/' + user + '/.ssh/'
#print ssh_path

# minidom解析xml文件
dom = xml.dom.minidom.parse(xml_file)
# 获取xml根节点
root = dom.documentElement
# 获取主节点和从节点主机名或IP
host_to = root.getElementsByTagName('to')[0].childNodes[0].data
host_from = root.getElementsByTagName('from')[0].childNodes[0].data


# 生成主节点ssh-key 配置免密码访问本机
# 并测试传输authorized_keys文件
def sshConnectMaster(hf):
	os.system('ssh-keygen -t rsa')
	os.system('cat ' + ssh_path + 'id_rsa.pub >> ' + ssh_path + 'authorized_keys')
	print "teset for localhost ssh connect"
	os.system('scp ' + ssh_path + 'authorized_keys ' + hf + ':'+ ssh_path)


# 处理从节点
def sshConnectSlave(ht):
	list_ht = ht.split(',')
	global host_name
	for host_name in list_ht:
		sshKeygenHost(host_name)
		time.sleep(1)
		sshKeytransHost(host_name)


# 远程主机创建.ssh目录
def sshKeygenHost(hn):
	ssh = paramiko.SSHClient()
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	#print (hn, port, user, passwd)
	ssh.connect(hn, port, user, passwd)
	stdin, stdout, stderr = ssh.exec_command('[ ! -d .ssh ] && mkdir .ssh')
	ssh.close()

# 远程主机拷贝authorized_keys
def sshKeytransHost(hn):
	trans = paramiko.Transport((hn, port))
	trans.connect(username = user, password = passwd)
	sftp = paramiko.SFTPClient.from_transport(trans)
	print "scp %sauthorized_keys %s:%s" % (ssh_path, hn, ssh_path)
	file_from = ssh_path + 'authorized_keys'
	sftp.put(file_from, os.path.join(ssh_path, 'authorized_keys'))
	trans.close()



if __name__ == "__main__":
	sshConnectMaster(host_from)
	sshConnectSlave(host_to)
	#pass
