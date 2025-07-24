import requests
import json
import time
import pandas as pd
import random
import string
import uuid
import os



def random_alnum(length: int, uppercase: bool = False) -> str:
    chars = string.ascii_uppercase + string.digits if uppercase else string.ascii_letters + string.digits
    return ''.join(random.choices(chars, k=length))

def random_cookie_header(num_pairs: int = 5, name_len: int = 8, value_len: int = 16) -> str:
    pairs = [f"{random_alnum(name_len)}={random_alnum(value_len)}" for _ in range(num_pairs)]
    return "; ".join(pairs)

def random_uuid4() -> str:
    return str(uuid.uuid4())

def random_fsn(length: int = 16) -> str:
    return random_alnum(length, uppercase=True)

def random_request_id(fsn: str = None, fsn_len: int = 16) -> str:
    return f"{random_uuid4()}.{fsn if fsn else random_fsn(fsn_len)}"

def fetch_product_json(product_id: str, pincode: str, proxies=None):
    # generate random cookies
    cookie_header = random_cookie_header(num_pairs=10, name_len=8, value_len=32)
    cookies = dict(pair.split('=', 1) for pair in cookie_header.split('; '))

    headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json',
        'Origin': 'https://www.flipkart.com',
        'Referer': 'https://www.flipkart.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
        'X-User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36 FKUA/website/42/website/Desktop',
        'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        # 'Cookie': '...',
    }
    # inject random Cookie header and random Request-ID
    headers['Cookie'] = cookie_header
    headers['REQUEST-ID'] = random_request_id(fsn=product_id)

    # 1) build the Flipkart product page URI for your JSON API
    page_uri = (
        "/noise-colorfit-icon-2-1-8-display-bluetooth-calling-"
        f"ai-voice-assistant-smartwatch/p/itm8229e27c1df28?pid={product_id}"
    )


    payload = {
        "pageUri": page_uri,
        "locationContext": {"pincode": str(pincode)},
        "isReloadRequest": True
    }


    # 4) POST to the Flipkart JSON API
    session = requests.Session()
    max_retries = 2
    for attempt in range(1, max_retries + 1):
        resp = session.post(
            "https://2.rome.api.flipkart.com/api/4/page/fetch",
            headers=headers,
            json=payload,
            cookies=cookies,
            proxies=proxies or {},
            timeout=30,
            # verify='zyte-ca.crt'
            verify = False
        )


        # handle Zyte/520 Temporary errors
        if resp.status_code == 520:
            retry_after = int(resp.headers.get("Retry-After", 60))
            print(f"[{attempt}] 520 → retrying in {retry_after}s…")
            time.sleep(retry_after)
            continue
        resp.raise_for_status()
        break

    data = resp.json()


    with open(f"Flipkart_outputs/{product_id}_{pincode}.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


    return data

# function to extract required data points from the raw JSON
def extract_product_data(raw_json: dict, product_id: str, pincode: str) -> dict:
    page_data = raw_json.get('RESPONSE', {}).get('pageData', {})
    # Title and length
    try:
        title = page_data.get('pageContext', {}).get('seo', {}).get('title', '')
    except:
        title = ''
    
    
    title_len = len(title)
    # Pricing details
    pricing = page_data.get('pricing', {})
    try:
        mrp = page_data.get('pageContext', {}).get('fdpEventTracking', {}).get('events', {}).get('psi', {}).get('ppd', {}).get('mrp')
    except:
        mrp = None
    try:
        live_price = page_data.get('paginationContextMap', {}).get('nps', {}).get('pricing', {})
    except:
        # RESPONSE.pageData.paginationContextMap.nps.pricing
        live_price = ''
    
    # Availability status
    try:
        availability = page_data.get('pageContext', {}).get('fdpEventTracking', {}).get('events', {}).get('psi', {}).get('pls', {}).get('isAvailable')
    except:
        availability = None
    if availability:
        availability = 'Yes'
    else:
        availability = 'No'
    
    
    # Deal tag from special price
    try:
        deal_tag = page_data.get('pageContext', {}).get('fdpEventTracking', {}).get('events', {}).get('psi', {}).get('ppd', {}).get('isSpecialPrice')
    except:
        deal_tag = None
    if deal_tag:
        deal_tag = 'Yes'
    else:
        deal_tag = 'No'

    
    # RESPONSE.slots[10].widget.data.offerInfo.value.offerGroups[0].offers[1].value.tags
    for price in pricing.get('prices', []):
        if price.get('priceType') == 'SPECIAL_PRICE':
            deal_tag = price.get('name')
            break
    
    # Dynamic highlights extraction
    bullet_points = None
    try:
        # Search through all slots to find highlights
        slots = raw_json.get('RESPONSE', {}).get('slots', [])
        for slot in slots:
            if isinstance(slot, dict):
                widget = slot.get('widget', {})
                if isinstance(widget, dict):
                    data = widget.get('data', {})
                    if isinstance(data, dict):
                        # Check for highlights in the data
                        highlights = data.get('highlights', {})
                        if isinstance(highlights, dict):
                            value = highlights.get('value', {})
                            if isinstance(value, dict):
                                text = value.get('text')
                                if text:
                                    bullet_points = len(text)
                                    break
    except Exception as e:
        print(f"Error extracting highlights: {e}")
        bullet_points = None
    
        
    # Catalog media counts
    try:
        catalog_images = page_data.get('pageContext', {}).get('fdpEventTracking', {}).get('events', {}).get('psi', {}).get('pas', {}).get('imagesCount')
    except:
        catalog_images = None
    try:
        catalog_videos = page_data.get('pageContext', {}).get('fdpEventTracking', {}).get('events', {}).get('psi', {}).get('pas', {}).get('videosCount') or 0
    except:
        catalog_videos = None
    # Estimated delivery dates

    try:
        edd = page_data.get('pageContext', {}).get('trackingDataV2', {}).get('slaText')
    except Exception as e:
        edd = None
    edd_fresh = None
    # Number of variations
    variations = len(page_data.get('swatchInfo', {}).get('mandatorySwatchAttributes', []))
    # Ratings breakdown
    try:    
        one_star = page_data.get('pageContext', {}).get('fdpEventTracking', {}).get('commonContext', {}).get('pr', {}).get('individualRatingsCount', [])[4].get('ratingCount')
        two_star = page_data.get('pageContext', {}).get('fdpEventTracking', {}).get('commonContext', {}).get('pr', {}).get('individualRatingsCount', [])[3].get('ratingCount')
        three_star = page_data.get('pageContext', {}).get('fdpEventTracking', {}).get('commonContext', {}).get('pr', {}).get('individualRatingsCount', [])[2].get('ratingCount')
        total_ratings = page_data.get('pageContext', {}).get('fdpEventTracking', {}).get('commonContext', {}).get('pr', {}).get('ratingsCount')
    except:
        one_star = None
        two_star = None
        three_star = None
        total_ratings = None

    try:
        average_rating = page_data.get('pageContext', {}).get('fdpEventTracking', {}).get('commonContext', {}).get('pr', {}).get('rating')
    except:
        average_rating = None
    # BSR not provided
    sub_cat_bsr = None
    cat_bsr = None
    # Seller information

    try:
        sold_by = page_data.get('pageContext', {}).get('trackingDataV2', {}).get('sellerName')
    except:
        sold_by = None

    # Product description text from TEXT widget - also make this dynamic
    description = 'No'
    try:
        # Search through all slots to find description
        slots = raw_json.get('RESPONSE', {}).get('slots', [])
        for slot in slots:
            if isinstance(slot, dict):
                widget = slot.get('widget', {})
                if isinstance(widget, dict):
                    data = widget.get('data', {})
                    if isinstance(data, dict):
                        renderable_components = data.get('renderableComponents', [])
                        if isinstance(renderable_components, list) and renderable_components:
                            for component in renderable_components:
                                if isinstance(component, dict):
                                    value = component.get('value', {})
                                    if isinstance(value, dict):
                                        text = value.get('text')
                                        if text and len(text) > 50:  # Assume description is longer than 50 chars
                                            description = 'Yes'
                                            break
                        if description == 'Yes':
                            break
    except Exception as e:
        print(f"Error extracting description: {e}")
        description = 'No'

    # Optional fields not in JSON
    bxgy = 'No'
    try:
        slots = raw_json.get('RESPONSE', {}).get('slots', [])
        for slot in slots:
            widget = slot.get('widget', {})
            if not isinstance(widget, dict):
                continue
            data = widget.get('data', {})
            if not isinstance(data, dict) or 'offerInfo' not in data:
                continue
            offer_info = data.get('offerInfo', {}).get('value', {})
            for group in offer_info.get('offerGroups', []):
                for offer in group.get('offers', []):
                    tracking = offer.get('action', {}).get('tracking', {})
                    tags = offer.get('value', {}).get('tags', [])
                    if tracking.get('offerType') == 'Combo Offer' or 'Combo Offer' in tags:
                        bxgy = 'Yes'
                        break
                if bxgy == 'Yes':
                    break
            if bxgy == 'Yes':
                break
    except Exception:
        bxgy = 'No'

    # Detect A+ featureSetList presence
    a_plus = 'No'
    try:
        slots = raw_json.get('RESPONSE', {}).get('slots', [])
        for slot in slots:
            widget = slot.get('widget', {})
            if not isinstance(widget, dict):
                continue
            data = widget.get('data', {})
            if not isinstance(data, dict):
                continue
            if 'featureSetList' in data:
                a_plus = 'Yes'
                break
    except Exception:
        a_plus = 'No'

    sns = 'NA'
    # Detect coupon offers in raw JSON
    coupon = 'No'
    try:
        slots = raw_json.get('RESPONSE', {}).get('slots', [])
        for slot in slots:
            widget = slot.get('widget', {})
            if not isinstance(widget, dict):
                continue
            data = widget.get('data', {})
            if not isinstance(data, dict) or 'offerInfo' not in data:
                continue
            offer_info = data.get('offerInfo', {}).get('value', {})
            for group in offer_info.get('offerGroups', []):
                for offer in group.get('offers', []):
                    description1 = offer.get('value', {}).get('description', '')
                    if isinstance(description1, str) and 'coupon' in description1.lower():
                        coupon = 'Yes'
                        break
                if coupon == 'Yes':
                    break
            if coupon == 'Yes':
                break
    except Exception:
        coupon = 'No'
    # Number of other sellers
    try:
        seller_count = page_data.get('pageContext', {}).get('trackingDataV2', {}).get('sellerCount')
        other_sellers = seller_count - 1 if seller_count is not None else None
    except:
        other_sellers = None
    return {
        "Product ID": product_id,
        "Pincode": pincode,
        "Title Length": title_len,
        "MRP": mrp,
        "Live Price": live_price,
        "Availability": availability,
        "Deal Tag": deal_tag,
        "Title": title,
        "Bullet Points": bullet_points,
        "Count of Catalog Images": catalog_images,
        "Videos in Catalog": catalog_videos,
        "EDD": edd,
        "EDD_Fresh": edd_fresh,
        "Number of Variations": variations,
        "3 Star Ratings": three_star,
        "2 Star Ratings": two_star,
        "1 Star Ratings": one_star,
        "Total Ratings": total_ratings,
        "Ratings": average_rating,
        "Sub-Category BSR": sub_cat_bsr,
        "Category BSR": cat_bsr,
        "Sold By": sold_by,
        "Description": description,
        "BXGY": bxgy,
        "A+": a_plus,
        "SNS": sns,
        "Coupon": coupon,
        "Number of Other Sellers": other_sellers
    }

# if __name__ == "__main__":
#     # example tied into your Excel loop
#     df_urls = pd.read_excel("Hygiene Input Master (HIM).xlsx", sheet_name="Oshea - Flipkart Rule")
#     df_pins = pd.read_excel("Hygiene Input Master (HIM).xlsx", sheet_name="Pincodes")

#     print(df_urls.head())
#     print(df_pins.head())

#     proxies = {
#         "http":  "http://ef8ca12adcf240c5b9691d1c821bc333:@api.zyte.com:8011/",
#         "https": "http://ef8ca12adcf240c5b9691d1c821bc333:@api.zyte.com:8011/"
#     }

#     output_dir = "Flipkart_outputs"
#     os.makedirs(output_dir, exist_ok=True)
#     # Build set of already scraped (product_id, pincode) pairs
#     already_scraped = set()
#     for fname in os.listdir(output_dir):
#         if fname.endswith(".json"):
#             parts = fname.split("_")
#             if len(parts) >= 2:
#                 product_id = parts[0]
#                 pincode = parts[1].replace('.json', '')  # Remove .json extension
#                 already_scraped.add((product_id, pincode))


#     result = []
#     for files in os.listdir(output_dir):
#         if files.endswith(".json"):
#             with open(os.path.join(output_dir, files), "r", encoding="utf-8") as f:
#                 data = json.load(f)
#             record = extract_product_data(data, files.split("_")[0], files.split("_")[1])
#             result.append(record)
#     df_result = pd.DataFrame(result)
#     df_result.to_csv("Flipkart_outputs/Flipkart_output.csv", index=False)

if __name__ == "__main__":
    # example tied into your Excel loop
    df_urls = pd.read_excel("Hygiene Input Master (HIM).xlsx", sheet_name="Oshea - Flipkart Rule")
    df_pins = pd.read_excel("Hygiene Input Master (HIM).xlsx", sheet_name="Pincodes")

    proxies = {
        "http":  "http://ef8ca12adcf240c5b9691d1c821bc333:@api.zyte.com:8011/",
        "https": "http://ef8ca12adcf240c5b9691d1c821bc333:@api.zyte.com:8011/"
    }

    output_dir = "Flipkart_outputs"
    os.makedirs(output_dir, exist_ok=True)
    # Build set of already scraped (product_id, pincode) pairs
    already_scraped = set()
    for fname in os.listdir(output_dir):
        if fname.endswith(".json"):
            parts = fname.split("_")
            if len(parts) >= 2:
                product_id = parts[0]
                pincode = parts[1].replace('.json', '')  # Remove .json extension
                already_scraped.add((product_id, pincode))

    # create combinations of product IDs and pincodes
    input_df = df_urls[['FSN']].assign(key=1).merge(
        df_pins[['Pincode']].assign(key=1), on='key'
    ).drop('key', axis=1)
    # filter out already scraped combinations
    input_df = input_df[~input_df.apply(
        lambda row: (row['FSN'], str(row['Pincode'])) in already_scraped,
        axis=1
    )]

    # fetch product json for each remaining input
    for _, row in input_df.iterrows():
        pid = str(row['FSN'])
        pin = str(row['Pincode'])
        try:
            fetch_product_json(pid, pin, proxies)
        except:
            continue
        
    result = []
    for files in os.listdir(output_dir):
        if files.endswith(".json"):
            with open(os.path.join(output_dir, files), "r", encoding="utf-8") as f:
                data = json.load(f)
            record = extract_product_data(data, files.split("_")[0], files.split("_")[1])
            result.append(record)
    df_result = pd.DataFrame(result)
    df_result.to_csv("Flipkart_outputs/Flipkart_output.csv", index=False)
    

    # For Testing
    
    # proxies = {
    #     "http":  "http://ef8ca12adcf240c5b9691d1c821bc333:@api.zyte.com:8011/",
    #     "https": "http://ef8ca12adcf240c5b9691d1c821bc333:@api.zyte.com:8011/"
    # }

    # pid="ATADRWRBYGRFRHRE"
    # pin="400013"
    # data = fetch_product_json(pid, pin, proxies)
    # record = extract_product_data(data,pid,pin)
    # print(record)