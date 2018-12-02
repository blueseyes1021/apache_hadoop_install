apache_hadoop_install
=====================

### 搭建apache版本hadoop集群环境（脚本化）

**注意：使用此工具需在 master 节点安装 python 的 paramiko 模块**
```
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
```

1. 编辑各主机配置文件 (debain linux)
```bash
# 修改主机名
sudo vi /etc/hostname
# 配置主机名和 ip 对应关系
sudo vi /etc/hosts
```

2. 配置 only-host 模式互访 (虚拟机)
```properties
# 设置静态固定 ip
sudo vi /etc/network/interfaces
# 添加网卡enp0s3配置
auto enp0s3
iface enp0s3 inet static
address 192.168.100.101
netmask 255.255.255.0
network 192.168.100.0
broadcast 192.168.100.255
```

3. 在 master 节点安装 paramiko 模块
```bash
# for python3
sudo apt install python3-pip
# 安装 paramiko 模块
# 参考 http://www.paramiko.org/installing.html
sudo pip3 install paramiko
```

4. 配置 ssh 免密码互访
```bash
# 编辑 ssh_no_passwd.xml
<sshconnect>
    <from>master01</from>
    <to>slave01,slave02,slave03</to>
    <comments>主节点向从节点ssh免密码登陆</comments>
</sshconnect>

# 配置 root 用户 ssh 登录许可
vi /etc/ssh/sshd_conf
# 修改
PermitRootLogin yes

# 使用 root 用户执行 ssh_no_passwd.py
su - root
./ssh_no_passwd.py your_root_passwd ssh_no_passwd.xml
# 一路回车(3次)，最后显示
scp /root/.ssh/authorized_keys slave01:/root/.ssh/
scp /root/.ssh/authorized_keys slave02:/root/.ssh/
scp /root/.ssh/authorized_keys slave03:/root/.ssh/

# 测试免密 ssh 访问
ssh slave01

# 配置ssh免密码访问成功后显示如下
The authenticity of host 'slave01 (192.168.1.201)' can't be established.
ECDSA key fingerprint is SHA256:RyvkYPT3IUzDZGqRB2cAMd6RNwdNXRVwBdZvgRUiLEg.
Are you sure you want to continue connecting (yes/no)? yes
Warning: Permanently added 'slave01,192.168.1.201' (ECDSA) to the list of known hosts.
Welcome to Ubuntu 16.04.1 LTS
```

5. 安装 hadoop 组件
```bash
# 配置参数
vi hadoop_install.cfg
# 安装 hadoop 相关组件
./hadoop_install.py -i
# 详细帮助
./hadoop_install.py -h
```

6. 启动 hadoop 服务
```bash
# 格式化 hdfs
su - root
# 切换到 hdfs 用户 (此用户作为 hdfs 文件系统的管理者)
su - hdfs
# 格式化命令
hadoop namenode -format  (同 hdfs namenode -formata)

# 继续以 hdfs 用户创建目录并授权
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
```

### 附 hdfs 帮助手册
```bash
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
```
