import time
import pandas as pd
import requests
from requests import RequestException
from retry import retry
from tqdm import tqdm  # 用于显示进度条
from config import cookies,headers


# 常量定义
BASE_URL = "https://redsky.target.com/redsky_aggregations/v1/web/"
API_KEY = "9f36aeafbe60771e321a7cc95a78140772ab3e96"
PROMO_ID = "477750868"
VISITOR_ID = "0198A148A2560201A726A213ABA675F5"
STORE_IDS = "1771,1768,1113,3374,1792"
ZIP_CODE = "52404"
MAX_PAGES = 50  # 最大爬取页数
PRODUCTS_PER_PAGE = 24  # 每页产品数量
REQUEST_DELAY = 3  # 请求间隔(秒)
TIMEOUT = 10  # 请求超时时间(秒)


cookies = cookies
headers = headers


@retry(RequestException, tries=5, delay=5)
def fetch_products_page(page_num):
    """获取指定页码的产品数据

    参数:
        page_num (int): 页码(从0开始)

    返回:
        list: 产品数据列表

    异常:
        抛出RequestException当请求失败时
    """
    try:
        params = {
            'key': API_KEY,
            'channel': 'WEB',
            'count': str(PRODUCTS_PER_PAGE),
            'default_purchasability_filter': 'true',
            'include_dmc_dmr': 'true',
            'include_sponsored': 'true',
            'include_review_summarization': 'false',
            'new_search': 'true',
            'offset': page_num * PRODUCTS_PER_PAGE,
            'page': f'/pl/{PROMO_ID}',
            'platform': 'desktop',
            'pricing_store_id': '1771',
            'promo_id': PROMO_ID,
            'scheduled_delivery_store_id': '1771',
            'spellcheck': 'true',
            'store_ids': STORE_IDS,
            'useragent': headers.get('user-agent', ''),
            'visitor_id': VISITOR_ID,
            'zip': ZIP_CODE,
        }

        response = requests.get(
            f"{BASE_URL}plp_search_v2",
            params=params,
            cookies=cookies,
            headers=headers,
            timeout=TIMEOUT
        )
        response.raise_for_status()
        return response.json().get('data', {}).get('search', {}).get('products', [])
    except Exception as e:
        print(f"获取第{page_num + 1}页产品数据时出错: {str(e)}")
        raise


def extract_product_info(products):
    """从产品数据中提取关键信息

    参数:
        products (list): 产品数据列表

    返回:
        list: 包含提取信息的字典列表
    """
    extracted_data = []
    for product in products:
        item = product.get('item', {})
        price = product.get('price', {})
        ratings = product.get('ratings_and_reviews', {}).get('statistics', {}).get('rating', {})

        product_info = {
            '产品ID': product.get('tcin'),
            '产品名称': item.get('product_description', {}).get('title'),
            '价格': price.get('current_retail'),
            '原价': price.get('reg_retail'),
            '格式化价格': price.get('formatted_current_price'),
            '单位价格': price.get('formatted_unit_price'),
            '购买链接': item.get('enrichment', {}).get('buy_url'),
            '促销信息': ' | '.join(
                [promo.get('plp_message') for promo in product.get('promotions', []) if promo.get('plp_message')]),
            '商品类型': item.get('product_classification', {}).get('item_type', {}).get('name'),
            '发布日期': item.get('mmbv_content', {}).get('street_date'),
            '评分': ratings.get('average'),
            '评价数量': ratings.get('count'),
        }
        extracted_data.append(product_info)

    return extracted_data


def save_to_excel(data, filename=f'{PROMO_ID}_target_product.xlsx'):
    """将数据保存为Excel文件

    参数:
        data (list): 要保存的数据
        filename (str): 输出文件名
    """
    try:
        df = pd.DataFrame(data)
        df.to_excel(filename, index=False)
        print(f"\n共获取 {len(df)} 条产品数据，已保存到 {filename}")
    except Exception as e:
        print(f"保存Excel文件失败: {str(e)}")
        raise


def main():
    """主函数，协调整个爬取流程"""
    extracted_products = []  # 存储所有产品信息

    print("开始获取产品数据...")

    # 使用tqdm显示进度条
    for page_num in tqdm(range(MAX_PAGES), desc="正在获取产品页面"):
        try:
            products = fetch_products_page(page_num)
            if not products:  # 如果没有数据，提前终止
                break

            extracted_products.extend(extract_product_info(products))
            time.sleep(REQUEST_DELAY)  # 请求间隔
        except Exception as e:
            print(f"处理第{page_num + 1}页时出错: {e}")
            continue

    # 保存结果
    save_to_excel(extracted_products)


if __name__ == "__main__":
    main()