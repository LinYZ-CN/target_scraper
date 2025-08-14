from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import requests
import base64
from retry import retry
from tqdm import tqdm  # 用于显示进度条
import time  # 用于请求间隔
from requests.exceptions import RequestException

from config import cookies,headers

# 常量定义
BASE_URL = "https://redsky.target.com/redsky_aggregations/v1/web/"
CIRCLE_PAGE_KEY = "9f36aeafbe60771e321a7cc95a78140772ab3e96"
CIRCLE_OFFERS_KEY = "eb2551e4accc14f38cc42d32fbc2b2ea"
VISITOR_ID = "019887F14366020183189E463146C0E5"
MAX_WORKERS = 15  # 最大线程数
REQUEST_DELAY = 0.5  # 请求间隔(秒)
TIMEOUT = 10  # 请求超时时间(秒)

cookies = cookies
headers = headers


@retry(RequestException, tries=5, delay=3)
def fetch_circle_page(page_num):
    """获取Circle优惠分页数据

    参数:
        page_num (int): 页码(作为偏移量使用)

    返回:
        dict: 包含优惠信息的JSON响应数据

    异常:
        抛出RequestException当请求失败时
    """
    try:
        # 生成并编码page_token参数
        page_token = f"{page_num}:deals_offer_grid_discount_0"
        encoded_token = base64.b64encode(page_token.encode("utf-8")).decode("utf-8")

        params = {
            'key': CIRCLE_PAGE_KEY,
            'visitor_id': VISITOR_ID,
            'placement_id': 'deals_offer_grid_discount',
            'pricing_store_id': '2109',
            'page': '/c/atb3q',
            'member_id': '10092084020',
            'page_token': encoded_token,
            'SapphireChannel': 'WEB',
        }

        response = requests.get(
            f"{BASE_URL}get_circle_noncircle_recommended_promotions_v1",
            params=params,
            cookies=cookies,
            headers=headers,
            timeout=TIMEOUT
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"请求第 {page_num} 页时出错: {str(e)}")
        raise


@retry(RequestException, tries=5, delay=3)
def fetch_circle_offers(circle_id):
    """获取单个Circle优惠的详细信息

    参数:
        circle_id (str): Circle优惠ID

    返回:
        dict: 包含优惠详情的JSON响应数据

    异常:
        抛出RequestException当请求失败时
    """
    try:
        params = {
            'key': CIRCLE_OFFERS_KEY,
            'visitor_id': VISITOR_ID,
            'offer_id': str(circle_id),
            'placement_id': 'circleweb_odp_relatedoffers',
            'channel': 'WEB',
            'page': 'odp',
        }

        response = requests.get(
            f"{BASE_URL}recommended_circle_offers_v1",
            params=params,
            cookies=cookies,
            headers=headers,
            timeout=TIMEOUT
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"获取优惠卷 {circle_id} 详情出错: {str(e)}")
        raise


def process_circle_deals(deals, seen_ids, all_circle_offers):
    """处理Circle优惠数据，提取有用信息

    参数:
        deals (list): 原始优惠数据列表
        seen_ids (set): 已处理的优惠ID集合
        all_circle_offers (list): 存储所有优惠信息的列表

    返回:
        list: 新发现的优惠ID列表
    """
    new_circle_ids = []
    for deal in deals:
        if deal['offer_type'] == 'circle':
            circle = deal['circle_offer']
            circle_id = circle.get('id')
            if circle_id not in seen_ids:
                seen_ids.add(circle_id)
                all_circle_offers.append({
                    '优惠类型': 'Circle优惠',
                    '活动ID': circle_id,
                    '活动标题': circle.get('title'),
                    '优惠价值': circle.get('value'),
                    '过期日期': circle.get('expiration_date'),
                    '兑换限制': circle.get('redemption_limit'),
                    '适用渠道': circle.get('channel')
                })
                new_circle_ids.append(circle_id)
    return new_circle_ids


def process_circle_offers(data, seen_ids, all_circle_offers):
    """处理Circle优惠详情数据

    参数:
        data (dict): 优惠详情响应数据
        seen_ids (set): 已处理的优惠ID集合
        all_circle_offers (list): 存储所有优惠信息的列表
    """
    circle_offers = data.get('data', {}).get('recommended_circle_offers', {}).get('circle_offers', [])
    for offer in circle_offers:
        offer_id = offer.get('id')
        if offer_id not in seen_ids:
            seen_ids.add(offer_id)
            all_circle_offers.append({
                '优惠类型': 'Circle优惠',
                '活动ID': offer_id,
                '活动标题': offer.get('title'),
                '优惠价值': offer.get('value'),
                '过期日期': offer.get('expiration_date'),
                '兑换限制': offer.get('redemption_limit'),
                '适用渠道': offer.get('channel')
            })


def save_to_csv(data, filename='优惠卷.csv'):
    """将数据保存为CSV文件

    参数:
        data (list): 要保存的数据列表
        filename (str): 输出文件名
    """
    try:
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)
        print(f"\n共获取 {len(df)} 条Circle优惠数据，已保存到 {filename}")
    except Exception as e:
        print(f"保存CSV文件失败: {str(e)}")
        raise


def main():
    """主函数，协调整个爬取流程"""
    all_circle_offers = []  # 存储所有优惠信息
    seen_ids = set()  # 用于去重的ID集合
    all_circle_ids = []  # 存储所有优惠ID
    page_num = 0  # 起始页码

    # 第一阶段：顺序获取所有Circle优惠ID
    print("开始获取Circle优惠页面...")
    with tqdm(desc="正在获取Circle优惠页面", unit="页") as pbar:
        while True:
            try:
                data = fetch_circle_page(page_num)
                deals = data.get('data', {}).get('recommended_deals', {}).get('deals', [])

                if not deals:  # 没有数据时终止循环
                    break

                # 处理当前页的优惠数据
                new_ids = process_circle_deals(deals, seen_ids, all_circle_offers)
                all_circle_ids.extend(new_ids)

                page_num += 20  # 页数增量
                pbar.update(1)
                time.sleep(REQUEST_DELAY)  # 请求间隔
            except Exception as e:
                print(f"获取第{page_num}页Circle优惠时出错: {e}")
                page_num += 20  # 出错时也增加页数
                continue

    # 第二阶段：并发获取Circle优惠详情
    print("\n开始获取优惠详情...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 创建未来任务字典 {future: circle_id}
        future_to_id = {executor.submit(fetch_circle_offers, cid): cid
                        for cid in all_circle_ids}

        # 使用tqdm显示进度
        for future in tqdm(as_completed(future_to_id),
                           total=len(future_to_id),
                           desc="正在获取优惠卷详情"):
            cid = future_to_id[future]
            try:
                data = future.result()
                process_circle_offers(data, seen_ids, all_circle_offers)
            except Exception as e:
                print(f"处理优惠卷 {cid} 时出错: {e}")

    # 保存结果
    save_to_csv(all_circle_offers)


if __name__ == '__main__':
    main()