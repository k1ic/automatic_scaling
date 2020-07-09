#coding=utf-8
#create by ck at 20200527, run at py37 on 10.41.23.176
import json, os, sys, time, random
import numpy as np
import requests, json
from requests.adapters import HTTPAdapter

from common import Log
log = Log(os.path.splitext(os.path.basename(__file__))[0]).getlog()

pool_names = ['pool1', 'pool2', 'pool3']
last_scaled_file = '../data/ec_last_scale_detail.lock'

#omp扩容接口
def call_omp_scale_api(pool_name = '', num = 0, qps = 0):
    res_return = True

    url = 'http://i.imgateway.xxx.com/omp/xxxbusiness/scale'

    payload = {
        'appKey': 4110003,
        'poolName': pool_name,
        'instanceNumber': num,
        'qps': int(qps)
    }

    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=3))

    log.info("before call i.imgateway.xxx.com scale_api|request_params:%s", payload)
    res = s.post(url, data=payload, timeout=2)
    log.info("after call i.imgateway.xxx.com scale_api elapsed:%f(ms)|http_code:%d|request_params:%s|res.text:%s", res.elapsed.microseconds/1000, res.status_code, payload, res.text)
    res_data = json.loads(res.text)
    if res.status_code != 200 or ('result' in res_data and  int(res_data['result']) != 100000) or ('code' in res_data and int(res_data['code']) != 10000):
        log.error("call i.imgateway.xxx.com scale_api failed,url:%s|elapsed:%f(ms)|http_code:%d|request_params:%s|res.text:%s", url, res.elapsed.microseconds/1000, res.status_code, payload, res.text)
        res_return = False

    return res_return

#omp缩容接口
def call_omp_shrink_api(pool_name = '', num = 0, qps = 0):
    res_return = True

    url = 'http://i.imgateway.xxx.com/omp/xxxbusiness/shrink'

    payload = {
        'appKey': 4110003,
        'poolName': pool_name,
        'instanceNumber': num,
        'qps': int(qps)
    }

    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=3))

    log.info("before call i.imgateway.xxx.com shrink_api|request_params:%s", payload)
    res = s.post(url, data=payload, timeout=2)
    log.info("after call i.imgateway.xxx.com shrink_api elapsed:%f(ms)|http_code:%d|request_params:%s|res.text:%s", res.elapsed.microseconds/1000, res.status_code, payload, res.text)
    res_data = json.loads(res.text)
    if res.status_code != 200 or ('result' in res_data and int(res_data['result']) != 100000) or ('code' in res_data and int(res_data['code']) != 10000):
    #if res.status_code != 200 or int(res_data['result']) != 10000:
        log.error("call i.imgateway.xxx.com shrink_api failed,url:%s|elapsed:%f(ms)|http_code:%d|request_params:%s|res.text:%s", url, res.elapsed.microseconds/1000, res.status_code, payload, res.text)
        res_return = False

    return res_return

#获取上一次扩缩容结果
def get_last_scale_result():
    if os.path.exists(last_scaled_file):
        with open(last_scaled_file, 'r', encoding='utf-8') as f:
            last_scale_plan = json.loads(f.read())
    else:
        last_scale_plan = {}
        for i, v in enumerate(pool_names):
            last_scale_plan[v] = 0

    return last_scale_plan

#记录本次扩缩容结果
def save_scale_result(scale_plan):
    f = open(last_scaled_file, 'wt', encoding='utf-8')
    f.write(json.dumps(scale_plan))
    f.close()

    return True

#对整个业务扩容
def call_omp_scaleup(num = 0, qps = 0):
    real_scaleup_total = 0

    #生成扩容计划
    last_scale_plan, scale_plan = get_last_scale_result(), get_last_scale_result()

    last_scale_num = 0
    for k, v in last_scale_plan.items():
        last_scale_num += v
    need_scale_num = num - last_scale_num

    if need_scale_num == 0:
        return num

    for i in range(need_scale_num):
        scale_plan[pool_names[i%len(pool_names)]] += 1

    #调用omp扩容接口
    for k, v in scale_plan.items():
        if v > 0 and v > last_scale_plan[k]:
            res_call = call_omp_scale_api(k, v, qps)
            log.info("call_omp_scale_api k:" + str(k) + '|v:' + str(v) + '|qps:' + str(qps) + '|res_call:' + str(res_call))
            if res_call == True:
                #扩容成功，累加本次扩容总台数
                real_scaleup_total += (v - last_scale_plan[k])
                log.info("real_scaleup_total:" + str(real_scaleup_total) + '|last_scale_plan[' + str(k) + ']:' + str(last_scale_plan[k]) + '|v:' + str(v))
            else:
                #该服务池本次扩容失败，扩容结果还原
                scale_plan[k] = last_scale_plan[k]

    #扩容结果写入文件
    save_scale_result(scale_plan)
    log.info("call_omp_scaleup scale_plan:" + json.dumps(scale_plan) + "|real_scaleup_total:" + str(real_scaleup_total))

    if real_scaleup_total == 0:
        return False
    else:
        return last_scale_num + real_scaleup_total

#对整个业务缩容
def call_omp_scaledown(num = 0, qps = 0):
    real_scaledown_total = 0

    #获取上一次扩缩容结果
    last_scale_plan, scale_plan = get_last_scale_result(), get_last_scale_result()

    last_scale_num = 0
    for k, v in last_scale_plan.items():
        last_scale_num += v
    if last_scale_num == 0:
        return num

    need_scale_num = last_scale_num - num

    if need_scale_num == 0:
        return num

    #剔除不需要缩容的服务池
    for k in list(scale_plan.keys()):
        if scale_plan[k] == 0:
            scale_plan.pop(k)

    #生成本次缩容计划
    need_scaledown_pool_names = list(scale_plan.keys())
    while(need_scale_num > 0):
        for k, v in scale_plan.items():
            if v - 1 >= 0 and need_scale_num - 1 >= 0:
                scale_plan[k] = v - 1
                need_scale_num -= 1
            else:
                continue

    #调用omp缩容接口
    for k, v in scale_plan.items():
        if last_scale_plan[k] > v:
            res_call = call_omp_shrink_api(k, v, qps)
            if res_call == True:
                #缩容成功，累加本次缩容总台数
                real_scaledown_total += (last_scale_plan[k] - v)
            else:
                #该服务池本次缩容失败，缩容结果还原
                scale_plan[k] = last_scale_plan[k]

    #补齐被剔除的服务池，并记录缩容结果
    scale_plan_for_save = {}
    for i, v in enumerate(pool_names):
        if v in scale_plan:
            scale_plan_for_save[v] = scale_plan[v]
        else:
            scale_plan_for_save[v] = 0
    save_scale_result(scale_plan_for_save)
    log.info("call_omp_scaledown scale_plan_for_save:" + json.dumps(scale_plan_for_save) + "|real_scaledown_total:" + str(real_scaledown_total))

    if real_scaledown_total == 0:
        return False
    else:
        return last_scale_num - real_scaledown_total

#执行扩缩容操作
def do_scaled_omp(num = 0, qps = 0, is_scaled_up = True):
    if is_scaled_up == True:
        res = call_omp_scaleup(num, qps)
    else:
        res = call_omp_scaledown(num, qps)

    return res

def get_last_5min_avg_qps():
    total_avg_qps = 0

    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=3))

    res = s.get("http://xxx.com.cn/render?target=alias(group(aliasSub(groupByNode(stats_byhost.php_nginx_access.daogou_sc_xxx*.total.nginx_qps.*%2c1%2c%27sumSeries%27)%2c+%27(.*)%27%2c+%27daogou%27)%2caliasSub(groupByNode(stats_byhost.php_nginx_access.gouwu_sc_xxx*.total.nginx_qps.*%2c1%2c%27sumSeries%27)%2c+%27(.*)%27%2c+%27gouwu%27)%2caliasSub(groupByNode(stats_byhost.php_nginx_access.shop_sc_xxx*.total.nginx_qps.*%2c1%2c%27sumSeries%27)%2c+%27(.*)%27%2c+%27shop%27)%2caliasSub(groupByNode(stats_byhost.php_nginx_access.whds_sc_xxx*.total.nginx_qps.*%2c1%2c%27sumSeries%27)%2c+%27(.*)%27%2c+%27whds%27)%2caliasSub(groupByNode(stats_byhost.php_nginx_access.ap*_sc_xxx*.total.nginx_qps.*%2c1%2c%27sumSeries%27)%2c+%27(.*)%27%2c+%27api%27)%2caliasSub(groupByNode(stats_byhost.php_nginx_access.fashion_e_xxx*.total.nginx_qps.*%2c1%2c%27sumSeries%27)%2c+%27(.*)%27%2c+%27fashion%27)%2caliasSub(groupByNode(stats_byhost.php_nginx_access.inbound_e_xxx*.total.nginx_qps.*%2c1%2c%27sumSeries%27)%2c+%27(.*)%27%2c+%27inbound%27)%2caliasSub(groupByNode(stats_byhost.php_nginx_access.ds_e_xxx*.total.nginx_qps.*%2c1%2c%27sumSeries%27)%2c+%27(.*)%27%2c+%27ds%27)%2caliasSub(groupByNode(stats_byhost.php_nginx_access.zhibo_e_xxx*.total.nginx_qps.*%2c1%2c%27sumSeries%27)%2c+%27(.*)%27%2c+%27zhibo%27)%2caliasSub(groupByNode(stats_byhost.php_nginx_access.activity_e_xxx*.total.nginx_qps.*%2c1%2c%27sumSeries%27)%2c+%27(.*)%27%2c+%27activity%27)%2caliasSub(groupByNode(stats_byhost.php_nginx_access.car_xxx*.total.nginx_qps.*%2c1%2c%27sumSeries%27)%2c+%27(.*)%27%2c+%27car%27)%2caliasSub(groupByNode(stats_byhost.php_nginx_access.c_xxx*.total.nginx_qps.*%2c1%2c%27sumSeries%27)%2c+%27(.*)%27%2c+%27c%27))%2C%20%27ec-project-qps%27)&from=-6min&until=now&format=json&maxDataPoints=1920", timeout=0.2)
    log.info("call graphite api elapsed:%f(ms)|http_code:%d|res.text:%s", res.elapsed.microseconds/1000, res.status_code, res.text)

    if res.status_code != 200:
         log.warning("get qps data failed")


    res_data = json.loads(res.text)
    for i, v in enumerate(res_data):
        v['datapoints'].pop()
        total_avg_qps += np.mean(np.array(v['datapoints'])[:,0]) #删除最后一分钟的数据，该数据有可能为空

    return total_avg_qps

#获取会员业务过去5分钟的平均499量
def get_last_5min_avg_499():
    total_avg_499 = 0

    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=3))

    res = s.get("http://graphite.noc.intra.sina.com.cn/render?target=alias(group(aliasSub(groupByNode(stats_byhost.php_nginx_access.daogou_sc_xxx*.qps.http_4xx.499.hits%2c1%2c%27sumSeries%27)%2c+%27(.*)%27%2c+%27daogou%27)%2caliasSub(groupByNode(stats_byhost.php_nginx_access.gouwu_sc_xxx*.qps.http_4xx.499.hits%2c1%2c%27sumSeries%27)%2c+%27(.*)%27%2c+%27gouwu%27)%2caliasSub(groupByNode(stats_byhost.php_nginx_access.shop_sc_xxx*.qps.http_4xx.499.hits%2c1%2c%27sumSeries%27)%2c+%27(.*)%27%2c+%27shop%27)%2caliasSub(groupByNode(stats_byhost.php_nginx_access.whds_sc_xxx*.qps.http_4xx.499.hits%2c1%2c%27sumSeries%27)%2c+%27(.*)%27%2c+%27whds%27)%2caliasSub(groupByNode(stats_byhost.php_nginx_access.ap*_sc_xxx*.qps.http_4xx.499.hits%2c1%2c%27sumSeries%27)%2c+%27(.*)%27%2c+%27api%27)%2caliasSub(groupByNode(stats_byhost.php_nginx_access.fashion_e_xxx*.qps.http_4xx.499.hits%2c1%2c%27sumSeries%27)%2c+%27(.*)%27%2c+%27fashion%27)%2caliasSub(groupByNode(stats_byhost.php_nginx_access.inbound_e_xxx*.qps.http_4xx.499.hits%2c1%2c%27sumSeries%27)%2c+%27(.*)%27%2c+%27inbound%27)%2caliasSub(groupByNode(stats_byhost.php_nginx_access.ds_e_xxx*.qps.http_4xx.499.hits%2c1%2c%27sumSeries%27)%2c+%27(.*)%27%2c+%27ds%27)%2caliasSub(groupByNode(stats_byhost.php_nginx_access.zhibo_e_xxx*.qps.http_4xx.499.hits%2c1%2c%27sumSeries%27)%2c+%27(.*)%27%2c+%27zhibo%27)%2caliasSub(groupByNode(stats_byhost.php_nginx_access.activity_e_xxx*.qps.http_4xx.499.hits%2c1%2c%27sumSeries%27)%2c+%27(.*)%27%2c+%27activity%27)%2caliasSub(groupByNode(stats_byhost.php_nginx_access.car_xxx*.qps.http_4xx.499.hits%2c1%2c%27sumSeries%27)%2c+%27(.*)%27%2c+%27car%27)%2caliasSub(groupByNode(stats_byhost.php_nginx_access.c_xxx*.qps.http_4xx.499.hits%2c1%2c%27sumSeries%27)%2c+%27(.*)%27%2c+%27c%27))%2c+%27ec-project-qps%27)&from=-6min&until=now&format=json&maxDataPoints=1920", timeout=0.2)
    log.info("call graphite api 499 elapsed:%f(ms)|http_code:%d|res.text:%s", res.elapsed.microseconds/1000, res.status_code, res.text)

    if res.status_code != 200:
         log.warning("get 499 data failed")

    res_data = json.loads(res.text)
    for i, v in enumerate(res_data):
        v['datapoints'].pop() #最后一分钟的数据不准，删除之（可能是Null）
        t = np.array(v['datapoints'])[:,0]
        t = t[t!=None] #过滤None元素
        if t.size > 0:
            total_avg_499 += np.mean(t)

    return total_avg_499

#调用DCP扩缩容接口
def do_scaled(num = 0):
    res = True

    url = 'http://es.api.xxx.com:6969/service/2005291521430000'

    headers_put = {
        'Content-Type': "application/json"
    }

    param = json.loads('{"Sid":2005291521430000,"Disable":false,"Name":"ecom_php_enterprise_tc_scaled","Owner":"xyz","MailRecipients":["aa","ab","ac"],"Cpu":16,"Ram":32,"Sconfigs":[{"Name":"cloud","AddRetry":0,"AddTimeout":10,"RemoveRetry":0,"RemoveTimeout":10,"Attributes":{"cmdb_type":"Web计算","cpu":16,"data_category":"cloud_efficiency","data_size":100,"instanceType":"ecs.c5.4xlarge","not_init":true,"os":"m-2zegpdc9x4o0at0nbwo1","pool":357,"ram":32,"security_groups":"sg-256w726mi","vswitchid":"vsw-2zehu7x671l9i69k6s8az","zoneid":"cn-beijing-c"}},{"Name":"php_config","AddRetry":0,"AddTimeout":8,"RemoveRetry":0,"RemoveTimeout":8,"Attributes":{"service_pool":"php_enterprise_tc"}},{"Name":"php_scaleup","AddRetry":0,"AddTimeout":8,"RemoveRetry":0,"RemoveTimeout":8,"Attributes":{"service_pool":"php_enterprise_tc"}},{"Name":"php_lb","AddRetry":0,"AddTimeout":8,"RemoveRetry":0,"RemoveTimeout":8,"Attributes":{"service_pool":"php_enterprise_tc"}}],"AddStrategy":1,"RemoveStrategy":1,"PlutoPool":"Weibo_Enterprise","PlutoAppId":"0IwN3G0heN7fiFt0","PlutoAppKey":"p7Fn4Z2yw60g2m7xkH0K60UWQ49K3vxw","ScheduleType":"once","Jobs":[{"TimeOfDay":"","InstanceNum":0}],"Dependencies":[],"CreateTime":"2020-05-29T15:21:54.711807961+08:00","UpdateTime":"2020-05-29T15:48:27.459131005+08:00"}')
    param['Jobs'][0]['InstanceNum'] = num
    payload = json.dumps(param)

    cookies_put = {'SESSIONID':'112345678900987654345678987654'}

    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=3))

    res = s.put(url, data=payload, headers=headers_put, cookies=cookies_put, timeout=0.2)
    log.info("call es.api.xxx.com api elapsed:%f(ms)|http_code:%d|request_params:%s|res.text:%s", res.elapsed.microseconds/1000, res.status_code, payload, res.text)
    if res.status_code != 200:
        log.error("call scaled api failed")
        res = False

    return res

#考虑每分钟qps的趋势，作为加减机器的权重
def cale_scale_num(avg_qps, avg_499):
    res = 0

    scale_num = 0
    scale_num = int(round((avg_qps - 3000)/1000)) if avg_qps > 3000 else 0

    #499比例规则
    multiple = 0
    multiple = round(avg_499/avg_qps, 2) if avg_499 > 0 else 0

    res = scale_num + int(round(multiple*scale_num)) #按499占总qps的比例增加扩容台数
    res = res if res <= 20 else 20 #限制扩容上限

    return res

if __name__ == '__main__':
    #初始化最后一次扩容数文件，默认值为0，即未进行扩容
    if not os.path.exists('../data/ec_last_scaled_num.lock'):
       f = open('../data/ec_last_scaled_num.lock', 'wt', encoding='utf-8')
       f.write('0')

    while True:
        with open('../data/ec_last_scaled_num.lock', 'r', encoding='utf-8') as f:
            tmp = f.read()
            last_scaled_num = int(tmp) if len(tmp) > 0 else 0

        curr_avg_qps = get_last_5min_avg_qps()
        curr_avg_499 = get_last_5min_avg_499()
        instance_num = cale_scale_num(curr_avg_qps, curr_avg_499)
        log.info("curr_avg_qps:%f|curr_avg_499:%f|scale_instance_num:%d", curr_avg_qps, curr_avg_499, instance_num)

        #当前平均qps低于扩容水位线，且之前未进行扩缩容 或 当前qps水位线与上次扩容持平，无需扩容
        if (instance_num == 0 and last_scaled_num == 0) or (instance_num > 0 and instance_num == last_scaled_num):
            log.info("curr_avg_qps:%f|scale_instance_num:%d|last_scaled_num:%d|no_need_scaled loop will end after 60s", curr_avg_qps, instance_num, last_scaled_num)
            time.sleep(60)
            continue

        #res_scaled = do_scaled(instance_num)
        res_scaled = do_scaled_omp(instance_num, curr_avg_qps, instance_num > last_scaled_num)
        log.info("instance_num:%d|last_scaled_num:%d|curr_avg_qps:%f|res_scaled:%s", instance_num, last_scaled_num, curr_avg_qps, str(res_scaled))
        #if res_scaled != False:
        if type(res_scaled) == int:
            f = open('../data/ec_last_scaled_num.lock', 'wt', encoding='utf-8')
            f.write(str(res_scaled))
            f.close()

            log.info("curr_avg_qps:%f|scale_instance_num:%d|last_scaled_num:%d|scaled_begin and scaled will end after 300s", curr_avg_qps, instance_num, last_scaled_num)
            time.sleep(300) #等待扩容结束
            log.info("curr_avg_qps:%f|scale_instance_num:%d|last_scaled_num:%d|scaled_end loop will end after 60s", curr_avg_qps, instance_num, last_scaled_num)
        else:
            log.info("curr_avg_qps:%f|scale_instance_num:%d|last_scaled_num:%d|scaled_failed and loop will end after 60s", curr_avg_qps, instance_num, last_scaled_num)

        time.sleep(60)
        continue
