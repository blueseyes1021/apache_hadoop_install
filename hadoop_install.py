#!/usr/bin/python
# -*- coding: utf-8 -*-
# *************************************************************
# 程 序 名: hadoop_install.py
# 配置文件: hadoop_install.cfg
# 说    明: hadoop环境搭建及相关组件安装
# 创建时间：2016.09.30
# 更新时点：2016.12.18
# 更新内容：添加选项参数
#			hadoop_install.py -[cih] | -[adu] software_name
#			-c	==>	--clean
#			-i	==>	--install
#			-h	==>	--help
#			-a	==>	--add		software
#			-d	==>	--delete	software
#			-u	==>	--update	software
# 作    者：曲怀觞
# *************************************************************

import sys
import os
import re
# 需要安装 paramiko 模块
# ssh远程操作主机
import paramiko	
from getopt import getopt


def main(argv = None):
	'''
		主函数
	'''
	# 设置参数选项
	opts, args = getopt(sys.argv[1:], "a:cd:hiu:",						\
					[													\
						"add=", "clean", "delete=",						\
						"help", "install", "update="					\
					])
	argnum = len(opts)
	if not ( argnum == 1 or argnum == 2 ):
		sys.exit("Usage: hadoop_install.py -[cih] | -[adu] software_name")

	# 默认配置文件与安装脚本在同一目录下
	dict_conf = load_config("hadoop_install.cfg")


	for op, package_name in opts:

		if not package_name == '':
			try:
				link_name = re.search('^(\w+)', package_name).group(1)
				unpack_name = package_name.split(r'.t')[0]
			except:
				sys.exit("init link_name and uppack_name error!")

		if op in ( "-a", "--add" ):
			install_software(dict_conf, package_name,					\
				unpack_name, link_name)

		elif op in ( "-c", "--clean" ):
			for key in dict_conf.keys():
				if re.search(r'_PKG' ,key):
					package_name = dict_conf[key]
					try:
						link_name = re.search('^(\w+)', package_name)	\
										.group(1)
						unpack_name = package_name.split(r'.t')[0]

						clean_software(dict_conf, package_name,			\
							unpack_name, link_name)
					except:
						pass

			for host in dict_conf['all_hosts'].split(','):
				if host == dict_conf['nn_host']:
					print '-' * 32
					for command in operate_dir(dict_conf, 'rm -rf'):
						print command
						os.system(command)
					for command in clean_user(dict_conf):
						print command
						os.system(command)

				else:
					ssh = paramiko.SSHClient()
					ssh.set_missing_host_key_policy(					\
						paramiko.AutoAddPolicy())
					ssh.connect(host, int(dict_conf['PORT']),			\
						dict_conf['USER'], dict_conf['PASSWD'])

					print '-' * 32
					for command in operate_dir(dict_conf, 'rm -rf'):
						print command
						ssh.exec_command(command)
					for command in clean_user(dict_conf):
						print command
						ssh.exec_command(command)


		elif op in ( "-d", "--delete" ):
			clean_software(dict_conf, package_name,						\
				unpack_name, link_name)
			pass

		elif op in ( "-h", "--help" ):
			help_print()
			sys.exit()

		elif op in ( "-i", "--install" ):
			for key in dict_conf.keys():
				if re.search(r'_PKG' ,key):
					package_name = dict_conf[key]
					try:
						link_name = re.search('^(\w+)', package_name)	\
										.group(1)
						unpack_name = package_name.split(r'.t')[0]

						install_software(dict_conf, package_name,		\
							unpack_name, link_name)
					except:
						pass

			init_hadoop(dict_conf)

		elif op in ( "-u", "--update" ):
			pass

		else:
			sys.exit("There is no other options")



def help_print():
	'''
		输出帮助信息
	'''
	print "# -------------------------------------------------"
	print "# Usage: hadoop_install.py -[cih] | -[adu] software"
	print "# -------------------------------------------------"
	print "# 卸载hadoop:	-c or --clean"
	print "# 安装hadoop:	-i or --install"
	print "# 帮助:		-h or --help"
	print "# 添加组件:	-a or --add software"
	print "# 删除组件:	-d or --delete software"
	print "# 更新组件:	-u or --update software"
	print "# -------------------------------------------------"
	print "# 配置文件：	hadoop_install.cfg"
	print "# -------------------------------------------------"



def load_config(cfg):
	'''
		读取hadoop安装配置文件，并生成配置信息字典。
		input: hadoop_install.cfg
		return: dict_conf
	'''
	d = {}
	fh = open(cfg, 'r')
	for line in fh:
		if re.match('^#', line):	# 过滤注释
			next
		if re.match("^$", line):	# 过滤空行
			next
		else:						# 生成配置信息字典
			try:
				( key, value ) = line.strip('\n').split(':')
				d[key] = value
			except:
				pass

	return d



def install_software(dict_conf, software, unpack_name, link_name):
	'''
		安装hadoop及组件
		遍历主从节点
		执行各步骤返回的指令
	'''
	for host in dict_conf['all_hosts'].split(','):
		print "Begin to install " + software
		if host == dict_conf['nn_host']:
			if not os.path.exists(dict_conf['INSTALL_PATH'] \
				+ unpack_name):
				print '-' * 32

				# 解压软件包
				try:
					os.system(uncompress_software(software, \
						dict_conf['SOFTWARE_PATH'],			\
						dict_conf['INSTALL_PATH']))
					print "uncompress " + software + " done!"
				except:
					sys.exit("uncompress " + software + " error")

				# 创建软链接
				try:
					os.system(link_software(unpack_name,				\
						dict_conf['LINK_HOME'],							\
						dict_conf['INSTALL_PATH'],					\
						link_name))		
					print "create " + unpack_name + " link done!"
				except:
					sys.exit("create " + unpack_name + " error")

				# 创建自启动环境变量
				try:
					os.system(profiled_software(software,				\
						dict_conf['LINK_HOME'],							\
						link_name,										\
						dict_conf['PROFILED']))
					print "init " + software + " profile done!"
				except:
					sys.exit("init " + software + " profile error")
				print ''

		else:
			print '-' * 32
			try:
				os.system('scp ' + dict_conf['SOFTWARE_PATH']			\
						+ software + ' ' + host + ':'					\
						+ dict_conf['INSTALL_PATH'])	
				print 'scp ' + dict_conf['SOFTWARE_PATH']				\
						+ software + ' ' + host + ':'					\
						+ dict_conf['INSTALL_PATH']
			except:
				sys.exit('scp ' + software + ' to ' + host + " error")

			ssh = paramiko.SSHClient()
			ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
			ssh.connect(host, int(dict_conf['PORT']),					\
				dict_conf['USER'], dict_conf['PASSWD'])
			
			try:
				ssh.exec_command(uncompress_software(					\
					software, dict_conf['INSTALL_PATH'],				\
					dict_conf['INSTALL_PATH'],							\
					) + ' && rm -rf '			\
					+ dict_conf['INSTALL_PATH']	+ software)
				print "uncompress " + software + " done!"
			except:
				sys.exit("uncompress " + software + " error!")

			try:
				ssh.exec_command(link_software(							\
					unpack_name, dict_conf['LINK_HOME'],				\
					dict_conf['INSTALL_PATH'], link_name))
				print "create link " + link_name + " done!"
			except:
				print "create link " + link_name + " error!"

			try:
				ssh.exec_command(profiled_software(						\
					software, dict_conf['LINK_HOME'],					\
					link_name, dict_conf['PROFILED']))
				print "create profile " + link_name + " done!"
			except:
				print "create profile " + link_name + " error!"

			print ''

			ssh.close()



def init_hadoop(dict_conf):
	'''
		创建hadoop集群所需目录、用户等
		遍历主从节点
		执行各步骤返回的指令
	'''
	for host in dict_conf['all_hosts'].split(','):

		if host == dict_conf['nn_host']:
			print '-' * 32
			for command in operate_dir(dict_conf, 'mkdir -p'):
				print command
				os.system(command)
			for command in create_user(dict_conf):
				print command
				os.system(command)
			for command in chmod_user(dict_conf):
				print command
				os.system(command)
			for command in add_env(dict_conf):
				print command
				os.system(command)
			print ''

		else:
			ssh = paramiko.SSHClient()
			ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
			ssh.connect(host, int(dict_conf['PORT']),					\
							dict_conf['USER'], dict_conf['PASSWD'])

			print '-' * 32
			for command in operate_dir(dict_conf, 'mkdir -p'):
				print command
				ssh.exec_command(command)
			for command in create_user(dict_conf):
				print command
				ssh.exec_command(command)
			for command in chmod_user(dict_conf):
				print command
				ssh.exec_command(command)
			for command in add_env(dict_conf):
				print command
				ssh.exec_command(command)

			print ''

			ssh.close()



def uncompress_software(software, software_path, install_path):
	'''
		根据压缩文件类型
		自动返回解压缩安装指令
	'''
	if re.search(r'.tar.gz|.tgz', software):
		opt = '-zxf'
	elif re.search(r'.bz2', software):
		opt = '-jxf'

	uncompress_cmd = 'tar ' + opt + ' ' + software_path \
						+ software + ' -C ' + install_path

	return uncompress_cmd


def link_software(unpack_name, link_path, install_path, link_name):
	'''
		返回创建软件链接命令
		链接目录为 /usr/mylink/
	'''
	if re.match('jdk', unpack_name):
		link_name = 'java'

	link_cmd = 'if [ ! -d ' + link_path + ' ];then mkdir -p ' \
					+ link_path + ';fi && ln -s ' + install_path \
					+ unpack_name + ' ' + link_path + link_name

	return link_cmd


def profiled_software(software, link_path, link_name, profile_path):
	'''
		返回配置环境变量指令
		启动服务自动载入
	'''
	if re.match('jdk', software):
		path_home = r'JAVA_HOME'
		link_name = r'java'
	else:
		path_home = link_name.upper() + r'_HOME'

	profiled_cmd = 'echo export ' + path_home + '=' + link_path \
						+ link_name + ' > ' + profile_path		\
						+ link_name + r'.sh'

	return profiled_cmd


def operate_dir(dict_conf, opt):
	'''
		opt ==>	'mkdir -p'	创建目录
				'rm -rf'	删除目录
		返回操作hadoop相关工作目录指令，包括:
		1.NN_DATA_DIR
		2.SNN_DATA_DIR
		3.DN_DATA_DIR
		4.YARN_LOG_DIR
		5.HADOOP_LOG_DIR
		6.HADOOP_MAPRED_LOG_DIR
		7.YARN_PID_DIR
		8.HADOOP_PID_DIR
		9.HADOOP_MAPRED_PID_DIR	
	'''
	nn_data_dir_cmd = opt + ' ' + dict_conf['NN_DATA_DIR']
	snn_data_dir_cmd = opt + ' ' + dict_conf['SNN_DATA_DIR']
	dn_data_dir_cmd = opt + ' ' + dict_conf['DN_DATA_DIR']
	yarn_log_dir_cmd = opt + ' ' + dict_conf['YARN_LOG_DIR']
	hadoop_log_dir_cmd = opt + ' ' + dict_conf['HADOOP_LOG_DIR']
	hadoop_mapred_log_dir_cmd = opt + ' '								\
								+ dict_conf['HADOOP_MAPRED_LOG_DIR']
	yarn_pid_dir_cmd = opt + ' ' + dict_conf['YARN_PID_DIR']
	hadoop_pid_dir_cmd = opt + ' ' + dict_conf['HADOOP_PID_DIR']
	hadoop_mapred_pid_dir_cmd = opt + ' '								\
								+ dict_conf['HADOOP_MAPRED_PID_DIR']

	return (nn_data_dir_cmd,
			snn_data_dir_cmd,
			dn_data_dir_cmd,
			yarn_log_dir_cmd,
			hadoop_log_dir_cmd,
			hadoop_mapred_log_dir_cmd,
			yarn_pid_dir_cmd,
			hadoop_pid_dir_cmd,
			hadoop_mapred_pid_dir_cmd)



def create_user(dict_conf):
	'''
		返回hadoop创建组、用户的指令，包括:
		1.GROUP_HADOOP
		2.USER_YARN
		3.USER_HDFS
		4.USER_MAPRED
	'''
	group_hadoop_cmd = 'groupadd ' + dict_conf['GROUP_HADOOP']
	user_yarn_cmd = 'useradd -g ' + dict_conf['GROUP_HADOOP']			\
					+ ' ' + dict_conf['USER_YARN']
	user_hdfs_cmd = 'useradd -g ' + dict_conf['GROUP_HADOOP']			\
					+ ' ' + dict_conf['USER_HDFS']
	user_mapred_cmd = 'useradd -g ' + dict_conf['GROUP_HADOOP']			\
					+ ' ' + dict_conf['USER_MAPRED']

	return (group_hadoop_cmd,
			user_yarn_cmd,
			user_hdfs_cmd,
			user_mapred_cmd)



def clean_user(dict_conf):
	'''
		返回hadoop删除组、用户的指令，包括:
		1.GROUP_HADOOP
		2.USER_YARN
		3.USER_HDFS
		4.USER_MAPRED
	'''
	user_yarn_cmd = 'userdel ' + dict_conf['USER_YARN']
	user_hdfs_cmd = 'userdel ' + dict_conf['USER_HDFS']
	user_mapred_cmd = 'userdel ' + dict_conf['USER_MAPRED']
	group_hadoop_cmd = 'groupdel ' + dict_conf['GROUP_HADOOP']

	return (group_hadoop_cmd,
			user_yarn_cmd,
			user_hdfs_cmd,
			user_mapred_cmd)



def chmod_user(dict_conf):
	'''
		返回目录授权用户指令，包括:
									USER	GROUP
		1.NN_DATA_DIR			==>	HDFS	HADOOP
		2.SNN_DATA_DIR			==>	HDFS	HADOOP
		3.DN_DATA_DIR			==>	HDFS	HADOOP
		4.YARN_LOG_DIR			==>	YARN	HADOOP
		5.HADOOP_LOG_DIR		==>	HDFS	HADOOP
		6.HADOOP_MAPRED_LOG_DIR	==>	MAPRED	HADOOP
		7.YARN_PID_DIR			==>	YARN	HADOOP
		8.HADOOP_PID_DIR		==>	HDFS	HADOOP
		9.HADOOP_MAPRED_PID_DIR	==>	MAPRED	HADOOP
	'''
	nn_data_dir_cmd = 'chown ' + dict_conf['USER_HDFS'] + ':'			\
						+ dict_conf['GROUP_HADOOP'] + ' '				\
						+ dict_conf['NN_DATA_DIR']
	snn_data_dir_cmd = 'chown ' + dict_conf['USER_HDFS'] + ':'			\
						+ dict_conf['GROUP_HADOOP'] + ' '				\
						+ dict_conf['SNN_DATA_DIR']
	dn_data_dir_cmd = 'chown ' + dict_conf['USER_HDFS'] + ':'			\
						+ dict_conf['GROUP_HADOOP'] + ' '				\
						+ dict_conf['DN_DATA_DIR']
	yarn_log_dir_cmd = 'chown ' + dict_conf['USER_YARN'] + ':'			\
						+ dict_conf['GROUP_HADOOP'] + ' '				\
						+ dict_conf['YARN_LOG_DIR']
	hadoop_log_dir_cmd = 'chown ' + dict_conf['USER_HDFS'] + ':'		\
						+ dict_conf['GROUP_HADOOP'] + ' '				\
						+ dict_conf['HADOOP_LOG_DIR']
	hadoop_mapred_log_dir_cmd = 'chown '								\
						+ dict_conf['USER_MAPRED'] + ':'				\
								+ dict_conf['GROUP_HADOOP'] + ' '		\
								+ dict_conf['HADOOP_MAPRED_LOG_DIR']
	yarn_pid_dir_cmd = 'chown ' + dict_conf['USER_YARN'] + ':'			\
						+ dict_conf['GROUP_HADOOP'] + ' '				\
						+ dict_conf['YARN_PID_DIR']
	hadoop_pid_dir_cmd = 'chown ' + dict_conf['USER_HDFS'] + ':'		\
						+ dict_conf['GROUP_HADOOP'] + ' '				\
						+ dict_conf['HADOOP_PID_DIR']
	hadoop_mapred_pid_dir_cmd = 'chown ' + dict_conf['USER_MAPRED']		\
						+ ':' + dict_conf['GROUP_HADOOP'] + ' '			\
						+ dict_conf['HADOOP_MAPRED_PID_DIR']

	return (nn_data_dir_cmd,
			snn_data_dir_cmd,
			dn_data_dir_cmd,
			yarn_log_dir_cmd,
			hadoop_log_dir_cmd,
			hadoop_mapred_log_dir_cmd,
			yarn_pid_dir_cmd,
			hadoop_pid_dir_cmd,
			hadoop_mapred_pid_dir_cmd)


def add_env(dict_conf):
	'''
		返回添加env配置文件指令，包括:
		1.HADOOP_LOG_DIR		==>	hadoop-env.sh
		2.YARN_LOG_DIR			==>	yarn-env.sh
		3.HADOOP_MAPRED_LOG_DIR	==>	mapred-env.sh
		4.HADOOP_PID_DIR		==>	hadoop-env.sh
		5.YARN_PID_DIR			==>	yarn-env.sh
		6.HADOOP_MAPRED_PID_DIR	==>	mapred-env.sh	
	'''
	hadoop_log_dir_cmd = 'echo export HADOOP_LOG_DIR='					\
						+ dict_conf['HADOOP_LOG_DIR']					\
						+ ' >> /usr/mylink/hadoop/etc/hadoop/hadoop-env.sh'
	yarn_log_dir_cmd = 'echo export YARN_LOG_DIR='						\
						+ dict_conf['YARN_LOG_DIR']						\
						+ ' >> /usr/mylink/hadoop/etc/hadoop/yarn-env.sh'
	hadoop_mapred_log_dir_cmd = 'echo export HADOOP_MAPRED_LOG_DIR='	\
						+ dict_conf['HADOOP_MAPRED_LOG_DIR']			\
						+ ' >> /usr/mylink/hadoop/etc/hadoop/mapred-env.sh'
	hadoop_pid_dir_cmd = 'echo export HADOOP_PID_DIR='					\
						+ dict_conf['HADOOP_PID_DIR']					\
						+ ' >> /usr/mylink/hadoop/etc/hadoop/hadoop-env.sh'
	yarn_pid_dir_cmd = 'echo export YARN_PID_DIR='						\
						+ dict_conf['YARN_PID_DIR']						\
						+ ' >> /usr/mylink/hadoop/etc/hadoop/yarn-env.sh'
	hadoop_mapred_pid_dir_cmd = 'echo export HADOOP_MAPRED_PID_DIR='	\
						+ dict_conf['HADOOP_MAPRED_PID_DIR']			\
						+ ' >> /usr/mylink/hadoop/etc/hadoop/mapred-env.sh'
									
	return (hadoop_log_dir_cmd,
			yarn_log_dir_cmd,
			hadoop_mapred_log_dir_cmd,
			hadoop_pid_dir_cmd,
			yarn_pid_dir_cmd,
			hadoop_mapred_pid_dir_cmd)

def clean_software(dict_conf, software, unpack_name, link_name):
	'''
		1.清理/opt目录下软件
		2.删除/usr/mylink下链接
		3.删除/etc/profile.d目录下配置文件
	'''
	print 'Clean ' + software
	print 'Please waiting...'
	for host in dict_conf['all_hosts'].split(','):
		print "Begin to clean " + host
		print "-----------------------------------------------"

		if host == dict_conf['nn_host']:
			try:
				os.system('rm -rf ' + dict_conf['INSTALL_PATH']			\
					+ unpack_name)
				print "clean " + software + " done!"
			except:
				print "clean " + software + " error!"

			if re.match('jdk', software):
				try:
					os.system('rm -rf ' + dict_conf['LINK_HOME']		\
						+ 'java')
					print "clean java link done!"
				except:
					print "clean java link error!"
				try:
					os.system('rm -rf ' + dict_conf['PROFILED']			\
						+ r'java.sh')
					print "clean profile java.sh done!"
				except:
					print "clean profile java.sh error!"
			else:
				try:
					os.system('rm -rf ' + dict_conf['LINK_HOME']		\
						+ link_name)
					print "clean " + link_name + " link done!"
				except:
					print "clean " + link_name + " link error!"
				try:
					os.system('rm -rf ' + dict_conf['PROFILED']			\
						+ link_name	+ r'.sh')
					print "clean profile " + link_name + " link done!"
				except:
					print "clean profile " + link_name + " link error!"

		else:
			ssh = paramiko.SSHClient()
			ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
			ssh.connect(host, int(dict_conf['PORT']),					\
						dict_conf['USER'],								\
						dict_conf['PASSWD'])
			
			try:
				ssh.exec_command('rm -rf ' + dict_conf['INSTALL_PATH']	\
						+ unpack_name)
				print "clean " + software + " done!"
			except:
				print "clean " + software + " error!"

			if re.match('jdk', software):
				try:
					ssh.exec_command('rm -rf ' + dict_conf['LINK_HOME']	\
						+ 'java')
					print "clean java link done!"
				except:
					print "clean java link error!"
				try:
					ssh.exec_command('rm -rf ' + dict_conf['PROFILED']	\
						+ r'java.sh')
					print "clean profile java.sh done!"
				except:
					print "clean profile java.sh error!"

			else:
				try:
					ssh.exec_command('rm -rf ' + dict_conf['LINK_HOME']	\
						+ link_name)
					print "clean " + link_name + " link done!"
				except:
					print "clean " + link_name + " link error!"
				try:
					ssh.exec_command('rm -rf ' + dict_conf['PROFILED']	\
						+ link_name + r'.sh')
					print "clean profile " + link_name + " link done!"
				except:
					print "clean profile " + link_name + " link error!"

			ssh.close()

if __name__ == "__main__":
	sys.exit(main())
