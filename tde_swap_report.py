#!/usr/bin/env python
#coding:utf-8

'''
Created on 2016-05-16
@author: bergxu
Desc: 
'''
import os,sys
import subprocess
import json
import MySQLdb
import time,datetime
import commands
deployhome = os.getcwd()

disk_block_in = '902'
disk_block_out = '903'
swap_si = '925'
swap_so = '926'
count = '12'
cgiip = '127.0.0.1'

#数据库执行脚本
def sqlexec(sqlcmd):
    dbhost = "127.0.0.1"
    dbuser = "rtrs"
    dbpasswd = "rtrs"
    dbport = 3306
    dbname = "db_apollo_monitor"
    try:
        conn = MySQLdb.connect(user=dbuser, db=dbname, passwd=dbpasswd, port=dbport, charset='utf8', host=dbhost)
        cur = conn.cursor()
        cur.execute(sqlcmd)
        resset = cur.fetchall()
        cur.close()
        conn.commit()
        conn.close()
        return resset
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])

#主函数入口
def main():
    #获取setid
    setsql = "select distinct a.SetID from t_server_info a where a.SystemId = '165483' and a.ModId = '1232';"
    setres = sqlexec(setsql)

    #初始化sql文件，并打开
    os.system("> sqlreplace.sql") 
    sqlfile = open('sqlreplace.sql','aw')

    #因为tnm2的接口限制，ip一次性不能太多，所以按setid获取性能数据
    for setid in setres:
        instancesql = "select a.ServerLanIP from t_server_info a where a.SystemId = '165483' and a.ModId = '1232' and a.SetID = "+str(setid[0])+";"
        ipset = sqlexec(instancesql)
        iplist = '"'
        for i in range(0,len(ipset)):
            iplist = iplist + ipset[i][0] + '","'
        iplist = iplist[:-2]
        #iplist = '"10.209.18.152","10.209.18.153","10.209.18.154","10.224.155.12","10.224.155.31"'
    
        #获取磁盘disk_block_in的性能数据
        blockincurlcmd = "curl -d '{\"method\":\"getdata.get_servers_basic_attr\", \"params\":{\"iplist\":["+iplist+"], \"attr\":"+disk_block_in+", \"count\":"+count+"}}' 'http://"+cgiip+"/new_api/getdata.get_servers_basic_attr' 2>/dev/null"
        (blockinstatus, blockinoutput) = commands.getstatusoutput(blockincurlcmd)
        stamp = datetime.datetime.now()
        for (ip,valuelist) in json.loads(blockinoutput).items():
            for j in range(0,len(valuelist)):
                jstamp = stamp - datetime.timedelta(minutes=5*(len(valuelist)-1-j)+2)
                blockinsql = "replace into t_tde_ds_io_status values('"+str(ip)+"','"+str(valuelist[j])+"','disk_block_in','"+jstamp.strftime('%Y-%m-%d %H:%M:00')+"');\n"
                sqlfile.write(blockinsql)

        #获取磁盘disk_block_out的性能数据
        blockoutcurlcmd = "curl -d '{\"method\":\"getdata.get_servers_basic_attr\", \"params\":{\"iplist\":["+iplist+"], \"attr\":"+disk_block_out+", \"count\":"+count+"}}' 'http://"+cgiip+"/new_api/getdata.get_servers_basic_attr' 2>/dev/null"
        (blockoutstatus, blockoutoutput) = commands.getstatusoutput(blockoutcurlcmd)
        stamp = datetime.datetime.now()
        for (ip,valuelist) in json.loads(blockoutoutput).items():
            for j in range(0,len(valuelist)):
                jstamp = stamp - datetime.timedelta(minutes=5*(len(valuelist)-1-j)+2)
                blockoutsql = "replace into t_tde_ds_io_status values('"+str(ip)+"','"+str(valuelist[j])+"','disk_block_out','"+jstamp.strftime('%Y-%m-%d %H:%M:00')+"');\n"
                sqlfile.write(blockoutsql)

        #获取磁盘swap_si的性能数据
        swapsicurlcmd = "curl -d '{\"method\":\"getdata.get_servers_basic_attr\", \"params\":{\"iplist\":["+iplist+"], \"attr\":"+swap_si+", \"count\":"+count+"}}' 'http://"+cgiip+"/new_api/getdata.get_servers_basic_attr' 2>/dev/null"
        (swapsistatus, swapsioutput) = commands.getstatusoutput(swapsicurlcmd)
        stamp = datetime.datetime.now()
        for (ip,valuelist) in json.loads(swapsioutput).items():
            for j in range(0,len(valuelist)):
                jstamp = stamp - datetime.timedelta(minutes=5*(len(valuelist)-1-j)+2)
                swapsisql = "replace into t_tde_ds_io_status values('"+str(ip)+"','"+str(valuelist[j])+"','swap_si','"+jstamp.strftime('%Y-%m-%d %H:%M:00')+"');\n"
                sqlfile.write(swapsisql)

        #获取磁盘swap_so的性能数据
        swapsocurlcmd = "curl -d '{\"method\":\"getdata.get_servers_basic_attr\", \"params\":{\"iplist\":["+iplist+"], \"attr\":"+swap_so+", \"count\":"+count+"}}' 'http://"+cgiip+"/new_api/getdata.get_servers_basic_attr' 2>/dev/null"
        (swapsostatus, swapsooutput) = commands.getstatusoutput(swapsocurlcmd)
        stamp = datetime.datetime.now()
        for (ip,valuelist) in json.loads(swapsooutput).items():
            for j in range(0,len(valuelist)):
                jstamp = stamp - datetime.timedelta(minutes=5*(len(valuelist)-1-j)+2)
                swapsosql = "replace into t_tde_ds_io_status values('"+str(ip)+"','"+str(valuelist[j])+"','swap_so','"+jstamp.strftime('%Y-%m-%d %H:%M:00')+"');\n"
                sqlfile.write(swapsosql)

    #删除2小时前表的历史数据
    delsql="delete from t_tde_ds_io_status where statustime < date_sub(now(), INTERVAL 120 MINUTE);"
    sqlfile.write(delsql)    

    sqlfile.close()

    #执行获取的性能数据脚本，写入数据库
    os.system("mysql -h10.209.7.15 -P3306 db_apollo_monitor -urtrs -prtrs -D db_apollo_monitor <sqlreplace.sql")

if __name__ == '__main__':
    if len(sys.argv) != 1:
        print "\033[1;31;40m Usage : python tde_swap_report \033[0m"
    else:
        try:
            main()
        except Exception as e:
            print e
