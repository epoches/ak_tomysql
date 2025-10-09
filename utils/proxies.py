# 验证代理可用性及响应速度的示例
import requests
import time

# 从代理网站复制过来的IP和端口
proxy = {
    'http': 'http://120.26.93.254:80',
    'https': 'http://120.26.93.254:80'
}

try:
    start_time = time.time()
    # 使用一个可以返回你IP的网站进行测试
    response = requests.get('https://httpbin.org/ip', proxies=proxy, timeout=5)
    end_time = time.time()

    if response.status_code == 200:
        print(f"代理可用，响应IP为: {response.text}")
        print(f"响应时间：{end_time - start_time:.2f}秒")
    else:
        print("代理请求失败")
except:
    print("代理无效或连接超时")