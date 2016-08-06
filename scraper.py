import requests, time, csv, json, sqlite3

req = requests.Session()

existing_db = 'http://pastebin.com/raw/51KAK8nf'
reddit_url = 'https://www.reddit.com/r/datasets/new/.json?before=t3_'

headers = {'User-Agent':'Python script gathering data for research, will stop if asked; contact at: reddit.com/u/hypd09', 'Accept-Encoding': 'gzip', 'Content-Encoding': 'gzip'}

DB_FILE = 'data.sqlite'
conn = sqlite3.connect(DB_FILE)
c = conn.cursor()
c.execute("CREATE TABLE IF NOT EXISTS data (created_utc,author,domain,num_comments,score,title,id,is_self)")

c.execute('SELECT id FROM data ORDER BY created_utc DESC')
_id = c.fetchone()

if _id is None:
    file = req.get(existing_db)

    csvreader = csv.reader(file.text.split('\n'),delimiter=',', quotechar='"')
    for d in csvreader:
        print(d)
        c.execute('INSERT INTO data VALUES (?,?,?,?,?,?,?,?)',d)
    conn.commit()
    
c.execute('SELECT id FROM data ORDER BY created_utc DESC')
_id = c.fetchone()

if _id is None:
    print('Something up with the existing db')
    quit()

_id = _id[0]

time.sleep(2) #just in case
newData = json.loads(req.get(reddit_url+_id, headers=headers).text)['data']['children']

for post in newData:
    post = post['data']
    print(json.dumps(post,indent=4,sort_keys=True))
    postData = [post['created_utc'],post['author'],post['domain'],post['num_comments'],post['score'],post['title'],post['id'],post['is_self']]
    print(postData)
    c.execute('INSERT INTO data VALUES (?,?,?,?,?,?,?,?)',postData)
    
c.close()

