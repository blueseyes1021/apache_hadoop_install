#!/usr/bin/python
# -*- coding: utf-8 -*-
# *************************************************************
# 程 序 名：hadoop_configure.py
# 配置文件：
# 说    明：自定义生成hadoop各组件所需配置信息
# 创建日期：2016.12.12
# 更新日期：2017.01.31
# 更新内容：调整增、删、改xml配置信息程序结构
# 更新日期：2017.02.01
# 更新内容：美化输出的xml文件
# 作    者：曲怀觞
# *************************************************************


import sys
from xml.etree import ElementTree
from xml.dom.minidom import parse
from getopt import getopt
import os
import json


# ###########################################################################
# 函数名：main
# 输  入：none
# 返回值：none
# ###########################################################################
def main(argv = None):
    opts, args = getopt(sys.argv[1:], 'a:d:m:hp:',  \
                    [                               \
                        'add=', 'delete=',          \
                        'modify=', 'help', 'pretty' \
                    ])

    try:
        key = args[0]
    except:
        key = ''

    try:
        val = args[1]
    except:
        val = ''

    for op, filename in opts:
        if op == '-a':
            if ( not os.path.exists(filename) ):
                create_xml(filename)
            add_element(filename, key, val)
        elif op == '-d':
            dlt_element(filename, key)
        elif op == '-m':
            mdf_element(filename, key, val)
        elif op == '-h':
            print_help()
        elif op == '-p':
            pretty_xml(filename)
        else:
            sys.exit('Usage: ./cfg_hadoop_xml.py -[adm] xml name value')


# ###########################################################################
# 函数名：print_help
# 输  入：none
# 返回值：none
# ###########################################################################
def print_help():
    '''
        输出帮助信息
    '''
    print "# -------------------------------------------------"
    print "# 用      法： hadoop_configure.py                 "
    print "# -------------------------------------------------"
    print "# 添加键值对： -a xml name value                   "
    print "# 修改键值对： -d xml name                         "
    print "# 修改键值对： -m xml name value                   "
    print "# 帮      助： -h                                  "
    print "# 格式化输出： -p                                  "
    print "# -------------------------------------------------"
    print "# 配置文件：                                       "
    print "# -------------------------------------------------"


# ###########################################################################
# 函数名：create_xml
# 输  入：xml文件
# 返回值：
# ###########################################################################
def create_xml(xml):
    '''
        配置hadoop及组件下的xml文件，例如：
        <configuration>
            <property>
                <name>fs.defaultFS</name>
                <value>hdfs://localhost:8020</value>
            </property>
        </configuration>
    '''
    # 创建 configuration 元素
    try:
        cfg = ElementTree.Element('configuration')
        tree = ElementTree.ElementTree(cfg)
        tree.write(xml, 'utf-8')
    except:
        sys.exit('create ' + xml + ' error!')


# ###########################################################################
# 函数名：add_element
# 输  入：(xml, name, value)
# 返回值：none
# ###########################################################################
def add_element(xml, name, value):
    '''
        添加配置信息键值对
    '''
    tree = ElementTree.parse(xml)
    root = tree.getroot()

    # 添加 configuration 下子元素 property
    pprt = ElementTree.SubElement(root, 'property')
    # 添加 property 下子元素 name 并赋值
    Ename = ElementTree.SubElement(pprt, 'name')
    Ename.text = name
    # 添加 property 下子元素 value 并赋值
    Evalue = ElementTree.SubElement(pprt, 'value')
    Evalue.text = value

    tree.write(xml, 'utf-8')


# ###########################################################################
# 函数名：mdf_element
# 输  入：(xml, name, value)
# 返回值：none
# ###########################################################################
def mdf_element(xml, name, value):
    '''
        修改配置信息某键的值
    '''
    tree = ElementTree.parse(xml)
    root = tree.getroot()
    
    for pprt in root:
        for child in pprt:
            # 找到name值并修改value的值
            if ( child.text == name ):
                pprt.find('value').text = value
                print 'modify ' + name + ' key/values successfully!'

    tree.write(xml, 'utf8')


# ###########################################################################
# 函数名：dlt_element
# 输  入：(xml, name)
# 返回值：none
# ###########################################################################
def dlt_element(xml, name, value):
    '''
        删除配置信息键值对
    '''
    tree = ElementTree.parse(xml)
    root = tree.getroot()
    
    for pprt in root:
        for child in pprt:
        # 找到name所在键值对并删除
            if ( child.text == name ):
                try:
                    root.remove(pprt)
                    print "remove element %s successfully!" % name
                except:
                    sys.exit('remove element ' + name + ' error!')
    
    tree.write(xml, 'utf8')


# ###########################################################################
# 函数名：pretty_xml
# 输  入：(xml)
# 返回值：none
# ###########################################################################
def pretty_xml(xml):
    '''
        格式化xml文件
    '''
    dom = parse(xml)
    f = open(xml, 'w')
    dom.writexml(f, addindent = '\t', newl = '\n', encoding = 'utf-8')


# ###########################################################################
# 函数名：auto_configure
# 输  入：(cfg)
# 返回值：none
# ###########################################################################
def auto_configure(cfg):
    '''
        自动化配置
    '''


# ###########################################################################
# 程序入口
# ###########################################################################
if __name__ == "__main__":
    sys.exit(main())
