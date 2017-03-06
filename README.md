# apache_hadoop_install
搭建apache版本hadoop集群环境（脚本化）
注意：使用此工具需在master节点安装python的paramiko模块

从头搭建apache hadoop集群(虚拟机)
需安装openssh-server启动ssh服务
并开启root访问权限
主机信息：
主节点
    master01
从节点
    slave01
    slave02
    slave03

分别编辑各主机以下配置文件(debain linux)
修改主机名
/etc/hostname
配置主机名和ip对应关系
/etc/hosts

# debain 发行版的 linux
# 虚拟机下 配置only-host模式互访
# 编辑 /etc/network/interfaces 来设置静态固定ip

# 文件内容如下
# -------------------------------------------------------------
# interfaces(5) file used by ifup(8) and ifdown(8)
auto lo
iface lo inet loopback

# 添加网卡enp0s3配置
auto enp0s3
iface enp0s3 inet static
address 192.168.100.101
#gateway 192.168.100.1
netmask 255.255.255.0
network 192.168.100.0
broadcast 192.168.100.255
# -------------------------------------------------------------

# 安装python包管理工具pip
在master节点
sudo apt-get install python-pip
# 安装python的paramiko模块
参考http://www.paramiko.org/installing.html
sudo pip install paramiko
# 更新pip
sudo pip install --upgrade pip

# paramiko安装错误解决
sudo apt-get install libssl-dev python-dev libffi-dev
# 更新paramiko包
sudo pip install paramiko --upgrade

# 编辑ssh_no_passwd.xml
<sshconnect>
    <from>master01</from>
    <to>slave01,slave02,slave03</to>
    <comments>主节点向从节点ssh免密码登陆</comments>
</sshconnect>

# 注意：安装openssh-server后需要修改配置
# 使root用户可以ssh登录
vi /etc/ssh/sshd_conf
# 修改为 
PermitRootLogin yes


执行ssh_no_passwd.py
使用root用户方便后续安装软件
su - root
./ssh_no_passwd.py your_root_passwd ssh_no_passwd.xml
一路回车(3次)，最后显示
scp /root/.ssh/authorized_keys slave01:/root/.ssh/
scp /root/.ssh/authorized_keys slave02:/root/.ssh/
scp /root/.ssh/authorized_keys slave03:/root/.ssh/
测试配置结果
ssh slave01
# 配置ssh免密码访问成功后显示如下
The authenticity of host 'slave01 (192.168.1.201)' can't be established.
ECDSA key fingerprint is SHA256:RyvkYPT3IUzDZGqRB2cAMd6RNwdNXRVwBdZvgRUiLEg.
Are you sure you want to continue connecting (yes/no)? yes
Warning: Permanently added 'slave01,192.168.1.201' (ECDSA) to the list of known hosts.
Welcome to Ubuntu 16.04.1 LTS

# 准备安装hadoop组件
根据自己的环境编辑hadoop_install.cfg配置
执行安装
./hadoop_install.py -i

详细帮助请使用
./hadoop_install.py -h

# 帮助手册
Usage: hdfs [--config confdir] [--loglevel loglevel] COMMAND
       where COMMAND is one of:
dfs                  run a filesystem command on the file systems supported in Hadoop.
classpath            prints the classpath
namenode -format     format the DFS filesystem
secondarynamenode    run the DFS secondary namenode
namenode             run the DFS namenode
journalnode          run the DFS journalnode
zkfc                 run the ZK Failover Controller daemon
datanode             run a DFS datanode
dfsadmin             run a DFS admin client
haadmin              run a DFS HA admin client
fsck                 run a DFS filesystem checking utility
balancer             run a cluster balancing utility
jmxget               get JMX exported values from NameNode or DataNode.
mover                run a utility to move block replicas across
                     storage types
oiv                  apply the offline fsimage viewer to an fsimage
oiv_legacy           apply the offline fsimage viewer to an legacy fsimage
oev                  apply the offline edits viewer to an edits file
fetchdt              fetch a delegation token from the NameNode
getconf              get config values from configuration
groups               get the groups which users belong to
snapshotDiff         diff two snapshots of a directory or diff the
                     current directory contents with a snapshot
lsSnapshottableDir   list all snapshottable dirs owned by the current user
  					Use -help to see options
portmap              run a portmap service
nfs3                 run an NFS version 3 gateway
cacheadmin           configure the HDFS cache
crypto               configure HDFS encryption zones
storagepolicies      list/get/set block storage policies
version              print the version

# ***启动hadoop服务***
# 安装完成后格式化hdfs
su - root
# 切换到hdfs用户 以此用户作为hdfs文件系统的管理者
su - hdfs
# 格式化命令
hadoop namenode -format
# 同 hdfs namenode -format
# 继续以hdfs用户创建目录并授权
# 创建 /tmp 授权 hdfs:hadoop 775
hdfs dfs -mkdir /tmp
hdfs dfs -chown -R hdfs:hadoop /tmp
hdfs dfs -chmod -R g+rwx /tmp
# 添加用户spark到组hadoop中
usermod -G hadoop spark
# 创建 /user 授权 hdfs:supergroup
hdfs dfs -mkdir /user
# 创建 /user/用户自定义目录 用户:users
hdfs dfs -mkdir -p /user/spark
hdfs dfs -chown -R spark:users /user/spark
# mapred-site.xml中配置
# mapreduce.jobhistory.done-dir
# 创建 /tmp/hadoop-yarn/staging/history/done 
# 存放历史作业日志
hdfs dfs -mkdir -p /tmp/hadoop-yarn/staging/history/done
# ? 是否授权用户 mapred
# mapreduce.jobhistory.intermediate-done-dir
# 创建 /tmp/hadoop-yarn/staging/history/done_intermediate 
# 存放正在运行的作业日志
hdfs dfs -mkdir -p /tmp/hadoop-yarn/staging/history/done_intermediate
# yarn.app.mapreduce.am.staging-dir
# yarn管理的mapreduce作业存放路径
# mapreduce.jobhistory.joblist.cache.size
# hadoop的历史服务器的WEB UI 最多显示的作业记录数量
