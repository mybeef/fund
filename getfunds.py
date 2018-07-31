import pymysql
import requests
import json
import re
import threading
import random

class Fnd:
    def __init__(self,fund_code,fundsname,fund_type):
        self.fund_code = fund_code
        self.fundsname = fundsname
        self.fund_type = fund_type

    def fund_insert(self):
        fundsdb = pymysql.connect(host='localhost', port=3306, user='root', passwd='ybf666666', db='funds')
        cursor = fundsdb.cursor()  # 获取一个游标
        selectsql = """SELECT fund_code FROM FND_MASTER WHERE fund_code = %s"""
        selectdata = (self.fund_code)

        insertsql = """INSERT INTO FND_MASTER(FUND_CODE,
        FUNDSNAME,FUND_TYPE) 
        VALUES (%s,%s,%s)"""
        if not cursor.execute(selectsql, selectdata):
            insertdata = (self.fund_code,self.fundsname,self.fund_type)
            cursor.execute(insertsql,insertdata)
            print('正在写入：',self.fund_code)
            fundsdb.commit()
            cursor.close()
            fundsdb.close()

class get_proxy:
    def __init__(self):
        pass
    def proxies(self):
        proxies_type = ['socks5', 'anonymous']
        socks5 = ['118.244.236.27:1080', '116.31.124.104:1080','114.239.0.6:1080','219.128.78.134:1080','36.33.25.117:1080', '115.231.218.102:1080', '111.198.2.90:1080',
                  '103.21.141.51:1080', '125.107.242.186:1080', '118.190.119.109:1080', '218.75.70.3:1080']
        anonymous = ['121.10.1.181:8080', '218.3.131.244:808', '101.236.48.238:8080', '101.236.33.172:8080',
                     '101.236.19.113:8080', '101.236.63.167:8080', '101.236.53.103:8080', '101.236.32.123:8080','101.236.40.189:8080']
        proxy_type = random.choice(proxies_type)
        proxy_value = random.choice(locals()[proxy_type])
        return proxy_type,proxy_value


#主程序定义
page = 1
url = 'http://vip.stock.finance.sina.com.cn/fund_center/data/jsonp.php/IO.XSRV2.CallbackList[\'9GMHs9u70GY8iTsZ\']/NetValue_Service.getNetValueOpen?'
head = {}
head['User-Agent'] = 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Mobile Safari/537.36'


while page <= 176:
    data = {}
    data['page'] = page
    data['num'] = '40'
    data['sort'] = 'nav_date'
    data['asc'] = '0'
    data['ccode'] = ''
    data['type2'] = '0'
    data['type3'] = ''

    proxy = get_proxy()
    type,value = proxy.proxies()

    content = requests.get(url,proxies={type:value},params=data,headers=head)
    content = re.findall(r'IO.XSRV2.CallbackList\[\'9GMHs9u70GY8iTsZ\'\]\(\((.*?)\)\)',content.text)
    content = content[0]
    content = re.sub(r'total_num','"total_num"',content)
    content = re.sub(r'data','"data"',content)
    content = re.sub(r'symbol','"symbol"',content)
    content = re.sub(r'sname','"sname"',content)
    content = re.sub(r'per_nav','"per_nav"',content)
    content = re.sub(r'total_nav','"total_nav"',content)
    content = re.sub(r'yesterday_nav','"yesterday_nav"',content)
    content = re.sub(r'nav_rate','"nav_rate"',content)
    content = re.sub(r'nav_a','"nav_a"',content)
    content = re.sub(r'sg_states','"sg_states"',content)
    content = re.sub(r'nav_date','"nav_date"',content)
    content = re.sub(r'fund_manager','"fund_manager"',content)
    content = re.sub(r'jjlx','"jjlx"',content)
    content = re.sub(r'jjzfe','"jjzfe"',content)
    content = re.sub(r'exec_time','"exec_time"',content)
    content = re.sub(r'sort_time','"sort_time"',content)

    content = json.loads(content)
    funds = content['data']
    for i in funds:
        fund_code = i['symbol']
        fundsname = i['sname']
        fund_type = i['jjlx']
        fund = Fnd(fund_code,fundsname,fund_type)
        threading.Thread(target=fund.fund_insert())
    page += 1
