from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import requests
from retry import retry
from tqdm import tqdm
import time
from requests.exceptions import RequestException
from config import cookies,headers
# 常量定义
BASE_URL = "https://redsky.target.com/redsky_aggregations/v1/web/"
PRODUCTS_API_KEY = "9f36aeafbe60771e321a7cc95a78140772ab3e96"
LIQUOR_API_KEY = "eb2551e4accc14f38cc42d32fbc2b2ea"
VISITOR_ID = "019887F14366020183189E463146C0E5"
MEMBER_ID = "10092084020"
PRICING_STORE_ID = "2109"
STORE_IDS = "2109,2196,3420,2848,2188"  # 默认商店ID列表
MAX_WORKERS = 15  # 最大线程数
REQUEST_DELAY = 0.5  # 请求间隔(秒)
TIMEOUT = 10  # 请求超时时间(秒)


cookies = cookies
headers = headers


@retry(RequestException, tries=5, delay=2)
def fetch_products(promo_id):
    """获取普通产品列表数据

    参数:
        promo_id (str): 优惠活动ID

    返回:
        dict: 包含产品列表的JSON响应数据

    异常:
        抛出RequestException当请求失败时
    """
    try:
        params = {
            'key': PRODUCTS_API_KEY,
            'channel': 'WEB',
            'count': '24',
            'default_purchasability_filter': 'true',
            'include_dmc_dmr': 'true',
            'include_sponsored': 'true',
            'include_review_summarization': 'false',
            'member_id': MEMBER_ID,
            'new_search': 'true',
            'offset': '0',
            'page': f'/pl/{promo_id}',
            'platform': 'desktop',
            'pricing_store_id': PRICING_STORE_ID,
            'promo_id': str(promo_id),
            'spellcheck': 'true',
            'store_ids': STORE_IDS,
            'useragent': headers.get('user-agent', ''),
            'visitor_id': VISITOR_ID,
            'zip': '33195',
        }

        response = requests.get(
            f"{BASE_URL}plp_search_v2",
            params=params,
            cookies=cookies,
            headers=headers,
            timeout=TIMEOUT
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"获取普通产品列表 {promo_id} 时出错: {str(e)}")
        raise


@retry(RequestException, tries=5, delay=2)
def fetch_liquor(promo_id):
    """获取酒类产品列表数据

    参数:
        promo_id (str): 优惠活动ID

    返回:
        dict: 包含酒类产品列表的JSON响应数据

    异常:
        抛出RequestException当请求失败时
    """
    try:
        params = {
            'channel': 'WEB',
            'include_sponsored_recommendations': 'true',
            'key': LIQUOR_API_KEY,
            'member_id': MEMBER_ID,
            'offer_id': str(promo_id),
            'page_id': f'/circle/o/offer-details/-/{promo_id}',
            'placement_id': 'adaptive_odp_eligibleitems',
            'platform': 'desktop',
            'pricing_store_id': PRICING_STORE_ID,
            'purchasable_store_ids': STORE_IDS,
            'visitor_id': VISITOR_ID,
            'page': f'/circle/o/offer-details/-/{promo_id}',
        }

        response = requests.get(
            f"{BASE_URL}plp_search_v2",
            params=params,
            cookies=cookies,
            headers=headers,
            timeout=TIMEOUT
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"获取酒类产品列表 {promo_id} 时出错: {str(e)}")
        raise


@retry(RequestException, tries=5, delay=2)
def fetch_odp(odp_id):
    """获取ODP(Offer Details Page)产品数据

    参数:
        odp_id (str): ODP ID

    返回:
        dict: 包含产品列表的JSON响应数据

    异常:
        抛出RequestException当请求失败时
    """
    try:
        params = {
            'channel': 'WEB',
            'include_sponsored_recommendations': 'true',
            'key': LIQUOR_API_KEY,
            'offer_id': str(odp_id),
            'page_id': f'/circle/o/offer-details/-/{odp_id}',
            'placement_id': 'adaptive_odp_eligibleitems',
            'platform': 'desktop',
            'pricing_store_id': '1771',
            'purchasable_store_ids': '1771,1768,1113,3374,1792',
            'visitor_id': '0198A148A2560201A726A213ABA675F5',
            'page': f'/circle/o/offer-details/-/{odp_id}',
        }

        response = requests.get(
            f"{BASE_URL}odp_eligible_items_v1",
            params=params,
            cookies=cookies,
            headers=headers,
            timeout=TIMEOUT
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"获取ODP产品列表 {odp_id} 时出错: {str(e)}")
        raise


def fetch_products_or_liquor(promo_id):
    """获取产品列表，自动尝试多种API

    参数:
        promo_id (str): 优惠活动ID

    返回:
        tuple: (promo_id, 产品列表, 数据来源类型)
    """
    try:
        # 先尝试获取普通产品
        data = fetch_products(promo_id)
        products = data.get('data', {}).get('search', {}).get('products', [])
        if products:
            return promo_id, products, 'products'

        # 如果没有产品，尝试获取酒类产品
        data2 = fetch_liquor(promo_id)
        products2 = data2.get('data', {}).get('recommended_products', {}).get('products', [])
        if products2:
            return promo_id, products2, 'liquor'

        # 最后尝试ODP
        data3 = fetch_odp(promo_id)
        products3 = data3.get('data', {}).get('eligible_items', {}).get('products', [])
        if products3:
            return promo_id, products3, 'odp'

        return promo_id, [], 'empty'
    except Exception as e:
        print(f"获取促销ID {promo_id} 产品时异常: {e}")
        return promo_id, [], 'error'


def process_product_data(promo_id, products, source):
    """处理产品数据，提取关键信息

    参数:
        promo_id (str): 关联活动ID
        products (list): 产品列表
        source (str): 数据来源类型

    返回:
        list: 处理后的产品信息字典列表
    """
    processed_products = []

    for product in products:
        product_info = {
            '关联活动ID': promo_id,
            '产品ID': product.get('tcin', ''),
            '价格(美元)': product.get('price', {}).get('current_retail', ''),
            '购买链接': product.get('item', {}).get('enrichment', {}).get('buy_url', ''),
            '数据来源': source
        }

        # 根据数据来源提取不同的促销信息
        if source == 'products':
            promotions = product.get('promotions', [{}])
            product_info['促销信息'] = promotions[0].get('plp_message', '无') if promotions else '无'
        elif source == 'liquor' or source == 'odp':
            if product.get('circle_offers') and product['circle_offers'].get('circle_offer_details'):
                product_info['促销信息'] = product["circle_offers"]["circle_offer_details"][0]["message"][
                    "short_description"]
            else:
                product_info['促销信息'] = '无'

        processed_products.append(product_info)

    return processed_products


def main_fetch_products(df_circle):
    """主函数，获取并处理所有产品数据

    参数:
        df_circle (DataFrame): 包含优惠活动的DataFrame

    返回:
        DataFrame: 合并后的最终数据
    """
    promo_ids = df_circle['活动ID'].tolist()
    all_products = []

    print("开始获取产品数据...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 创建任务字典 {future: promo_id}
        futures = {executor.submit(fetch_products_or_liquor, pid): pid for pid in promo_ids}

        # 使用tqdm显示进度
        for future in tqdm(as_completed(futures), total=len(futures), desc="多线程获取产品数据"):
            promo_id, products, source = future.result()

            # 如果没有产品，添加空记录
            if source in ('error', 'empty') or not products:
                all_products.append({
                    '关联活动ID': promo_id,
                    '产品ID': '',
                    '价格(美元)': '',
                    '促销信息': '',
                    '购买链接': '',
                    '数据来源': source
                })
                continue

            # 处理产品数据
            processed = process_product_data(promo_id, products, source)
            all_products.extend(processed)

            # 请求间隔防止被封
            time.sleep(REQUEST_DELAY)

    # 创建DataFrame
    df_products = pd.DataFrame(all_products)
    print(f"\n共获取 {len(df_products)} 条产品数据")

    # 合并数据
    df_final = pd.merge(
        df_products,
        df_circle,
        left_on='关联活动ID',
        right_on='活动ID',
        how='left'
    )

    # 清理和保存数据
    df_final.drop(columns=['活动ID'], inplace=True)
    output_file = 'target_product.csv'
    df_final.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"合并后的数据已保存到 {output_file}")

    return df_final


if __name__ == "__main__":
    # 读取优惠卷数据
    try:
        # 优先尝试读取Excel文件，如果失败则尝试CSV
        try:
            df_circle = pd.read_excel('优惠卷.xlsx')
        except:
            df_circle = pd.read_csv('优惠卷.csv', encoding='utf-8-sig')

        # 执行主函数
        main_fetch_products(df_circle)
    except Exception as e:
        print(f"程序运行出错: {str(e)}")