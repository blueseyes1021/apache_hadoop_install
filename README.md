# apache_hadoop_install
搭建apache版本hadoop集群环境（脚本化）
注意：使用此工具需在master节点安装python的paramiko模块

# 从头搭建apache hadoop集群(虚拟机)
# 需安装openssh-server启动ssh服务
# 并开启root访问权限
主机信息：
主节点
    master01
从节点
    slave01
    slave02
    slave03

分别编辑各主机以下配置文件(debain linux)
# 修改主机名
/etc/hostname
# 配置主机名和ip对应关系
/etc/hosts

# 安装python包管理工具pip
# 在master节点
sudo apt-get install python-pip
# 安装python的paramiko模块
# 参考http://www.paramiko.org/installing.html
sudo pip install paramiko
# 更新pip
sudo pip install --upgrade pip

# 编辑ssh_no_passwd.xml
<sshconnect>
    <from>master01</from>
    <to>slave01,slave02,slave03</to>
    <comments>主节点向从节点ssh免密码登陆</comments>
</sshconnect>

# 执行ssh_no_passwd.py
# 使用root用户方便后续安装软件
su - root
./ssh_no_passwd.py your_root_passwd ssh_no_passwd.xml
# 一路回车(3次)，最后显示
scp /root/.ssh/authorized_keys slave01:/root/.ssh/
scp /root/.ssh/authorized_keys slave02:/root/.ssh/
scp /root/.ssh/authorized_keys slave03:/root/.ssh/
# 测试配置结果
ssh slave01
# 配置ssh免密码访问成功后显示如下
The authenticity of host 'slave01 (192.168.1.201)' can't be established.
ECDSA key fingerprint is SHA256:RyvkYPT3IUzDZGqRB2cAMd6RNwdNXRVwBdZvgRUiLEg.
Are you sure you want to continue connecting (yes/no)? yes
Warning: Permanently added 'slave01,192.168.1.201' (ECDSA) to the list of known hosts.
Welcome to Ubuntu 16.04.1 LTS

# 准备安装hadoop组件
# 根据自己的环境编辑hadoop_install.cfg配置
# 执行安装
./hadoop_install.py -i

# 详细帮助请使用
./hadoop_install.py -h
