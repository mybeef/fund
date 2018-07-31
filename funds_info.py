import requests
import pymysql
from bs4 import BeautifulSoup
import threading
import random
import datetime

class Fnd:
    fundsdb = pymysql.connect(host='localhost', port=3306, user='root', passwd='ybf666666', db='funds')
    list = ()
    fund_code = ''
    data_date = ''
    unit_net_chng_pct_1_week = ''
    unit_net_chng_pct_1_mon = ''
    unit_net_chng_pct_3_mon = ''
    unit_net_chng_pct_6_mon = ''
    unit_net_chng_pct_1_year = ''
    unit_net_chng_pct_2_year = ''
    unit_net_chng_pct_3_year = ''
    unit_net_chng_pct_tyear = ''

    def __init__(self):
        pass

    def fetch_fund_code(self):
        #获取数据库基金代码
        cursor = self.fundsdb.cursor()
        yesterday = (datetime.date.today() + datetime.timedelta(days=-1)).strftime('%Y%m%d')
        selectsql = """SELECT fund_code FROM fnd_master WHERE data_date IS NULL """ #data_date < %s OR
        cursor.execute(selectsql,yesterday)
        self.list = list(zip(cursor.fetchall()))[0]
        cursor.close()
        return self.list


    def update_fund_pct(self):
        # fundsdb = pymysql.connect(host='localhost', port=3306, user='root', passwd='ybf666666', db='funds')
        cursor = self.fundsdb.cursor()
        updatesql = """UPDATE fnd_master SET data_date = %s,
                        unit_net_chng_pct_1_week = %s,
                        unit_net_chng_pct_1_mon = %s,
                        unit_net_chng_pct_3_mon = %s,
                        unit_net_chng_pct_6_mon = %s,
                        unit_net_chng_pct_1_year = %s,
                        unit_net_chng_pct_2_year = %s,
                        unit_net_chng_pct_3_year = %s,
                        unit_net_chng_pct_tyear = %s
                        WHERE fund_code = %s"""
        update_data = (self.data_date,self.unit_net_chng_pct_1_week,self.unit_net_chng_pct_1_mon,self.unit_net_chng_pct_3_mon,self.unit_net_chng_pct_6_mon,self.unit_net_chng_pct_1_year,self.unit_net_chng_pct_2_year,self.unit_net_chng_pct_3_year,self.unit_net_chng_pct_tyear,self.fund_code)
        print(update_data)
        try :
            cursor.execute(updatesql,update_data)
            print('Updating fund:%s\n'%(self.fund_code))
        except pymysql.Error as e:
            print(e)

        self.fundsdb.commit()
        cursor.close()

class get_proxy:
    def __init__(self):
        pass

    def proxies(self):
        proxies_type = ['socks5', 'anonymous']
        socks5 = ['118.244.236.27:1080', '116.31.124.104:1080', '114.239.0.6:1080','114.217.96.104:1080', '219.128.78.134:1080',
                  '36.33.25.117:1080','	49.75.46.8:1080','115.231.218.102:1080','115.216.1.253:1080','111.198.2.90:1080','115.159.142.73:1080','222.93.252.123:10180',
                  '103.21.141.51:1080', '125.107.242.186:1080', '118.190.119.109:1080', '103.21.141.51:1080','218.75.70.3:1080','180.126.72.79:1080','183.68.142.78:1080','120.195.108.70:1080','120.26.66.53:1080']
        anonymous = ['121.10.1.181:8080', '218.3.131.244:808', '101.236.48.238:8080', '101.236.33.172:8080',
                     '101.236.19.113:8080', '101.236.63.167:8080', '101.236.53.103:8080', '	106.14.179.199:8080',
                     '101.236.40.189:8080','101.236.23.213:8080','101.236.48.238:8080','183.230.177.170:8081','123.57.76.102:80']
        proxy_type = random.choice(proxies_type)
        proxy_value = random.choice(locals()[proxy_type])
        return proxy_type, proxy_value
    def heads(self):
        heads = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134',
            'Mozilla/5.0(Macintosh;U;IntelMacOSX10_6_8;en-us)AppleWebKit/534.50(KHTML,likeGecko)Version/5.1Safari/534.50',
            'Mozilla/5.0(Windows;U;WindowsNT6.1;en-us)AppleWebKit/534.50(KHTML,likeGecko)Version/5.1Safari/534.50',
            'Mozilla/5.0(Macintosh;IntelMacOSX10.6;rv:2.0.1)Gecko/20100101Firefox/4.0.1',
            'Opera/9.80(Macintosh;IntelMacOSX10.6.8;U;en)Presto/2.8.131Version/11.11',
            'Mozilla/4.0(compatible;MSIE7.0;WindowsNT5.1;Maxthon2.0)',
            'Mozilla/4.0(compatible;MSIE7.0;WindowsNT5.1;TencentTraveler4.0)',"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
            "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
            "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
            "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
            "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
            "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
            "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
            "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
            "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)",
            "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E; LBBROWSER)",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 LBBROWSER",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)",
            "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; 360SE)",
            "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)"]
        head = random.choice(heads)
        return head


#定义链接


#获取基金代码列表
fnd = Fnd()
fund_list = fnd.fetch_fund_code()
proxy = get_proxy()

#根据基金代码列表抓取数据并调用类更新数据
for fund_code in fund_list:
    fnd.fund_code = fund_code
    url = 'http://fund.eastmoney.com/'+fund_code+'.html?spm=001.1.swh'

    type, value = proxy.proxies()
    head = proxy.heads()

    print(type,value)
    print('get fund:%s' % fund_code)
    content = requests.get(url,proxies = {type:value},headers = {'User-Agent':head}).content
    soup = BeautifulSoup(content,'html5lib')
    track = soup.find_all(name = 'div',attrs = {'class':'fund_item IncreaseAmount popTab'})[0]
    fnd.data_date = track.find_all(name = 'span',attrs = {'id':'jdzfDate'})[0].get_text()
    track_pect = track.find_all(name = 'tr',attrs = {'class':''})[1].find_all(name = 'div')
    if track_pect[1].get_text() != '--':
        fnd.unit_net_chng_pct_1_week = float('%.4f' % (float(track_pect[1].get_text().strip('%'))/100))
    else:
        fnd.unit_net_chng_pct_1_week = ''
    if track_pect[2].get_text() != '--':
        fnd.unit_net_chng_pct_1_mon =  float('%.4f' % (float(track_pect[2].get_text().strip('%'))/100))
    else:
        fnd.unit_net_chng_pct_1_mon = ''
    if track_pect[3].get_text() != '--':
        fnd.unit_net_chng_pct_3_mon =  float('%.4f' % (float(track_pect[3].get_text().strip('%'))/100))
    else:
        fnd.unit_net_chng_pct_3_mon = ''
    if track_pect[4].get_text() != '--':
        fnd.unit_net_chng_pct_6_mon =  float('%.4f' % (float(track_pect[4].get_text().strip('%'))/100))
    else:
        fnd.unit_net_chng_pct_6_mon = ''
    if track_pect[5].get_text() != '--':
        fnd.unit_net_chng_pct_1_year =  float('%.4f' % (float(track_pect[5].get_text().strip('%'))/100))
    else:
        fnd.unit_net_chng_pct_1_year = ''
    if track_pect[6].get_text() != '--':
        fnd.unit_net_chng_pct_2_year =  float('%.4f' % (float(track_pect[6].get_text().strip('%'))/100))
    else:
        fnd.unit_net_chng_pct_2_year = ''
    if track_pect[7].get_text() != '--':
        fnd.unit_net_chng_pct_3_year =  float('%.4f' % (float(track_pect[7].get_text().strip('%'))/100))
    else:
        fnd.unit_net_chng_pct_3_year = ''
    if track_pect[8].get_text() != '--':
        fnd.unit_net_chng_pct_tyear =  float('%.4f' % (float(track_pect[8].get_text().strip('%'))/100))
    else:
        fnd.unit_net_chng_pct_tyear = ''
    threading.Thread(target = fnd.update_fund_pct())
fnd.fundsdb.close()

