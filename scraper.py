import requests, time, csv, json, sqlite3

req = requests.Session()

existing_db = 'https://gist.githubusercontent.com/masterofpun/306d41f90a68e10f4d6cad58ea3db673/raw/05e9bfcfe30f732655ce28def80ddff5d8f4972a/r_datasets_6_aug_2016.csv'
reddit_url = 'https://www.reddit.com/r/datasets/new/.json?before=t3_'

headers = {'User-Agent':'Python script gathering data for research, will stop if asked; contact at: reddit.com/u/hypd09', 'Accept-Encoding': 'gzip', 'Content-Encoding': 'gzip'}

DB_FILE = 'data.sqlite'
conn = sqlite3.connect(DB_FILE)
c = conn.cursor()
c.execute("CREATE TABLE IF NOT EXISTS data (created_utc INTEGER,author TEXT,domain TEXT,num_comments INTEGER,score INTEGER,title TEXT,id TEXT UNIQUE,is_self TEXT,url TEXT,selftext TEXT)")

c.execute('SELECT id FROM data ORDER BY created_utc DESC')
_id = c.fetchone()

if _id is None:
    file = req.get(existing_db)

    csvreader = csv.reader(file.text.split('\n'),delimiter=',', quotechar='"')
    for d in csvreader:
        c.execute('INSERT INTO data VALUES (?,?,?,?,?,?,?,?,?,?)',d)
    print('added existing data')
    conn.commit()
        
    c.execute('SELECT id FROM data ORDER BY created_utc DESC')
    _id = c.fetchone()

if _id is None:
    print('Something up with the existing db')
    quit()

print(_id)
_id = _id[0]

time.sleep(2) #just in case
newData = json.loads(req.get(reddit_url+_id, headers=headers).text)['data']['children']

for post in newData:
    post = post['data']
    postData = [int(post['created_utc']),post['author'],post['domain'],post['num_comments'],post['score'],post['title'],post['id'],post['is_self'],post['url'],post['selftext']]
    print(postData)
    c.execute('INSERT INTO data VALUES (?,?,?,?,?,?,?,?,?,?)',postData)
    
conn.commit()
c.close()

