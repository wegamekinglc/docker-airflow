import collections

import pandas as pd
import sqlalchemy as sa

word_mapping = {
    '屏幕': ['屏幕', '副屏', '主屏', '显示屏', '黑屏', '闪屏', '锁屏', '触控'],
    '外观': ['外观', '面板', '外壳', '亚克力'],
    '打印': ['打印', '打印头', '打印机', '纸仓', '切刀', '热敏', '卡纸', '黑标', '挡板', '墨盒'],
    '电子秤': ['电子秤', '秤', '称重', '电子称', '称'],
    '声音': ['声音', '音量', '音箱', '音响', '喇叭', '语音', 'MIC', '电话'],
    '网络': ['网络', '无线网', 'wifi', '热点', '4g', '3g', '网', 'ip', '网线', '内网', '外网', '全网通', '移动', '联通', '电信', '流量', '联网'],
    '蓝牙': ['蓝牙'],
    '属性': ['字体', '分辨率', '黑体', '加粗', '尺寸', '温度', '图标'],
    '电池': ['电池', '电量', '充电', '充不上电', '充不了电'],
    '软件': ['软件', '硬件管家', '耗材商城', '应用市场', '应用', '盛付通', '收钱吧', '支付宝', '插件', '输入法'],
    '系统设置': ['初始化', '安全模式', '霸屏', '模式', '省电模式', '网络模式', '收银模式', 'root', '密码', '激活', '解绑', '快捷键', '更新'],
    '维修': ['维修', '保修', '报修', '修'],
    '卡顿': ['卡顿', '卡死', '死机', '慢'],
    '存储': ['存储', '缓存', '空间'],
    '外设': ['接口', '外接', 'usb', '打印机', '电子秤', '秤', '称重', '电子称', '称', '钱箱', '键盘', '摄像头', '相机', 'SD'],
    '刷卡': ['卡', 'SIM', 'IC', '会员卡', '银行卡', '物联卡', '金融卡', '积分卡', '非接卡', '刷卡器', '读卡器', '读写器'],
    '扫码': ['扫码', '扫码枪', '扫码盒', '扫描枪']
}


class Analysis:
    def __init__(self, window):
        self.connection = "postgresql+psycopg2://sunmiai:7jetVZJ0cZ7B25ei@pgm-bp199af1e28ooii114870.pg.rds.aliyuncs.com:3433/customer_service"
        self.window = window
        self.word_mapping = word_mapping
        self.data = self.pull_data()

    def pull_data(self):
        """
           Get data from pgsql
        """
        engine = sa.create_engine(self.connection)
        query = "select * from monitor where extract (day from (now() - qtime)) <= {}".format(self.window)
        df = pd.read_sql_query(query, engine)
        return df

    def product_stat(self):
        """
           Counts the products occur
        """
        products = []
        for product in self.data['matched_products']:
            products += product
        product_count = collections.Counter(products)
        df = pd.DataFrame([product_count]).T.reset_index()
        df.columns = ['product', 'number']
        return df

    def function_stat(self):
        """
            Counts the problem customers focus on
        """
        focus_count = {}
        for query in self.data['query']:
            query = query.lower().replace(' ', '')
            for component in word_mapping.keys():
                if any(word in query for word in word_mapping[component]):
                    focus_count[component] = focus_count[component] + 1 if focus_count.get(component) else 1
        df = pd.DataFrame([focus_count]).T.reset_index()
        df.columns = ['function', 'number']
        return df
