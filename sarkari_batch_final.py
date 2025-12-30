
import requests
from bs4 import BeautifulSoup
import re
import time
import pymongo
import certifi
from datetime import datetime
import os

# ===================== CONFIGURATION =====================
LISTING_URL = "https://sarkariresult.com.cm/result/"

# GitHub Actions se Secret uthayega.
MONGO_URI = os.getenv("MONGO_URI") 

# Safety: Agar env variable nahi mila (Local run ke liye), to error print karega
if not MONGO_URI:
    print("⚠️ Warning: MONGO_URI environment variable nahi mila.")
    # Agar local test karna hai to niche wali line uncomment karein:
    # MONGO_URI = "mongodb+srv://surajkannujiya517_db_user:GoIWSSzlnxRn23gB@cluster0.adwxc68.mongodb.net/sara?retryWrites=true&w=majority"

DB_NAME = "sara"
COLL_NAME = "records"
BATCH_LIMIT = 0  # 0 = Scrape ALL links

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# ===================== UTILS =====================
def clean_text(text):
    if not text: return ""
    return re.sub(r"\s+", " ", text).strip()

def remove_branding(text):
    if not text: return ""
    text = re.sub(r"(service\s*)?sarkari\s*(result|service|naukri)?(\.com|\.cm|\.im|\.co)?", "", text, flags=re.IGNORECASE)
    return clean_text(text.strip(" -|"))

def dedup_items(items):
    seen = set()
    out = []
    for i in items:
        key = i.lower()
        if key not in seen:
            seen.add(key)
            out.append(i)
    return out

# ===================== VALIDATOR =====================
def validate_link(url):
    if not url: return False
    url = url.lower()
    if any(x in url for x in ['telegram', 'whatsapp', 'facebook', 'instagram', 'youtube', 'channel']): return False
    if 'gov.in' in url or 'nic.in' in url: return True
    if 'sarkari' in url:
        if not url.endswith(('.pdf', '.jpg', '.doc', '.docx')): return False 
        try:
            r = requests.head(url, headers=HEADERS, allow_redirects=True, timeout=3)
            if 'text/html' in r.headers.get('Content-Type', '').lower(): return False
            return True
        except: return False
    return True

# ===================== CLASSIFIER =====================
def classify_list_by_content(items):
    content_str = " ".join(items).lower()
    if "start date" in content_str or "last date" in content_str or "exam date" in content_str: return "Important Dates"
    if ("general" in content_str and "obc" in content_str) or "₹" in content_str: 
        if "payment" not in content_str: return "Application Fee"
    if "debit card" in content_str or "credit card" in content_str: return "Payment Mode"
    if "minimum age" in content_str or "maximum age" in content_str: return "Age Limit"
    if "written exam" in content_str or "interview" in content_str: return "Selection Process"
    return "Details"

# ===================== SCRAPER =====================
def scrape_single_page(url):
    print(f"   ▶ Scraping: {url}...")
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(res.content, "html.parser")
        container = soup.find("div", class_="entry-content") or soup.find("article") or soup.body

        raw_title = clean_text(soup.title.string) if soup.title else "Unknown"
        clean_title = remove_branding(raw_title)

        record = {
            "title": clean_title,
            "typeOfPost": "RESULT",
            "nameOfPost": clean_title,
            "postDate": datetime.now(),
            "shortInformation": "",
            "data": [],
            "createdAt": datetime.now()
        }

        # Short Info
        full_text = container.get_text(" | ", strip=True)
        start_match = re.search(r'(Short Information|Short Details)\s*[:\-]', full_text, re.IGNORECASE)
        end_match = re.search(r'(Important Dates|Application Fee|Notification)', full_text, re.IGNORECASE)
        if start_match and end_match and end_match.start() > start_match.end():
            record['shortInformation'] = remove_branding(full_text[start_match.end():end_match.start()]).replace("|", "").strip()
        else:
             for p in container.find_all('p'):
                t = clean_text(p.get_text())
                if len(t) > 60 and "post date" not in t.lower() and "click here" not in t.lower():
                    record['shortInformation'] = remove_branding(t)
                    break

        # List Scanner
        for ul in container.find_all("ul"):
            items = []
            for li in ul.find_all("li"):
                t = remove_branding(li.get_text())
                if t: items.append(t)
            
            if not items: continue
            
            heading = classify_list_by_content(items)
            if heading == "Details":
                prev = ul.find_previous_sibling()
                if prev and prev.name in ['h2', 'h3', 'h4', 'h5', 'p']:
                    h_text = remove_branding(prev.get_text())
                    if len(h_text) < 50: heading = h_text

            if heading in ["Details", "Additional Information"]: continue
            
            if not any(d['title'] == heading for d in record['data']):
                record["data"].append({ "title": heading, "dataType": "list", "data": dedup_items(items) })

        # Table Scanner
        for table in container.find_all("table"):
            rows = table.find_all("tr")
            if not rows: continue

            is_link_table = False
            if len(rows) > 1:
                cells = rows[1].find_all("td")
                if len(cells) >= 2 and cells[1].find("a"): is_link_table = True

            if is_link_table:
                if any(d['title'] == "Important Links" for d in record['data']): continue
                link_data = []
                for tr in rows:
                    tds = tr.find_all("td")
                    if len(tds) >= 2:
                        name = remove_branding(tds[0].get_text())
                        if "whatsapp" in name.lower() or "telegram" in name.lower(): continue
                        a_tag = tds[1].find("a")
                        if a_tag and 'href' in a_tag.attrs:
                            if validate_link(a_tag['href']):
                                link_data.append({ "Link Name": name, "Link": f"<a href='{a_tag['href']}' target='_blank'>Click Here</a>" })
                if link_data:
                     record["data"].append({ "title": "Important Links", "dataType": "table", "columns": [{"name": "Link Name", "type": "text"}, {"name": "Link", "type": "html"}], "data": link_data })
            else:
                headers = [clean_text(th.get_text()) for th in rows[0].find_all(["th", "td"])]
                if not headers: continue
                header_slug = " ".join(headers).lower()

                section_title = None
                if "post name" in header_slug: section_title = "Vacancy Details"
                elif "selection" in header_slug: section_title = "Selection Process"
                
                if section_title and not any(d['title'] == section_title for d in record['data']):
                    data_rows = []
                    for tr in rows[1:]:
                        tds = tr.find_all("td")
                        if len(tds) == len(headers):
                            row = {}
                            for i, h in enumerate(headers):
                                row[h] = remove_branding(tds[i].get_text())
                            data_rows.append(row)
                    if data_rows:
                        record["data"].append({ "title": section_title, "dataType": "table", "columns": [{"name": h, "type": "text"} for h in headers], "data": data_rows })

        return record

    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return None

# ===================== MAIN LOOP =====================
def get_all_links(url):
    print(f"📡 Connecting to Listing Page: {url}")
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(res.content, "html.parser")
        container = soup.find("div", class_="entry-content") or soup.find("div", class_="post-content") or soup.body
        links = []
        for a in container.find_all("a", href=True):
            href = a["href"]
            txt = clean_text(a.get_text())
            if "sarkariresult.com.cm" in href and len(txt) > 10:
                if "page" in href or "category" in href: continue
                if href not in links: links.append(href)
        return links
    except: return []

if __name__ == "__main__":
    if not MONGO_URI:
        print("❌ Error: MONGO_URI set nahi hai. Exiting.")
        exit()

    client = pymongo.MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    db = client[DB_NAME]
    
    links = get_all_links(LISTING_URL)
    if not links:
        print("❌ No links found.")
        exit()

    targets = links if BATCH_LIMIT == 0 else links[:BATCH_LIMIT]
    print(f"🚀 Processing {len(targets)} pages... (Skip Duplicates Mode)\n")

    for link in targets:
        # Step 1: Scrape Data
        data = scrape_single_page(link)
        
        if data and data["data"]:
            # Step 2: DUPLICATE CHECK
            existing_record = db[COLL_NAME].find_one({"title": data["title"]})
            
            if existing_record:
                # Agar mil gaya, to SKIP karo
                print(f"   ⏩ Exists (Skipped): {data['title'][:40]}...")
            else:
                # Agar nahi mila, to INSERT karo
                db[COLL_NAME].insert_one(data)
                print(f"   ✅ Saved (New): {data['title'][:40]}...")
        else:
            print("⚠️ Skipped (Empty/Failed):", link)
        
        time.sleep(1)

    print("\n🏁 Process Completed!")
