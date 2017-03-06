#!/bin/sh
# #############################################################
# init_hdfs.sh
# 1.切换到root用户	su - root 
# 2.切换到hdfs用户	su - hdfs
# 先执行下面命令格式化hdfs的namenode
# hdfs namenode -format
# #############################################################

# 初始化hdfs系统工作目录
init_hdfs_workspace() {
	hdfs dfs -mkdir -p /tmp
	hdfs dfs -mkdir -p /tmp/hadoop-yarn/staging/history/done
	hdfs dfs -mkdir -p /tmp/hadoop-yarn/staging/history/done_intermediate
	hdfs dfs -chgrp -R hadoop /tmp
	hdfs dfs -chmod -R g+rwx /tmp
	usermod -G hadoop $1
	hdfs dfs -mkdir -p /user/$1
	hdfs dfs -chown -R $1:users /user/$1
	hdfs dfs -mkdir -p /mapred
	hdfs dfs -chown -R mapred:hadoop /mapred
	hdfs dfs -chmod -R g+rwx /mapred
}

# 函数统一调用
main() {
	init_hdfs_workspace $1
}

# 脚本入口
if [ $# -ne 1 ];then
	echo "Usage: ./init_hdfs.sh username"
	exit 0
fi
main $1
