# WOS
Simple tool to store and read dictionaries in bgz files.

# Installation
Clone this repository and use `pip install -e .` from that directory.

# Usage

## Writting 
under construction

## Reading
Import the package:
```python
import WOS
```

Setting up the paths to the data:
```python
from pathlib import Path
dataPath = Path("/gpfs/sciencegenome/WoS-disambiguation/")
WOSDataPath = dataPath / "WOSData.bgz"
UID2PositionPath = dataPath / "UID2Positions.bgz"
```


Reading the first 100 articles in order:
```python
# Reading articles in order
reader = WOS.DatabaseReader(WOSDataPath)

articles = reader.readNextArticles(100)

reader.close()
```

Reading using the random access API:
```python
#Copy the bgz to a local disk to improve performance
#Random access over network mounted devices is slow!

from tqdm.auto import tqdm

#Getting some random IDs
import random
IDSample = random.sample(list(UID2Position), 1000)


#Accessing and checking UID
reader = WOS.DatabaseReader(WOSDataPath)
for ID in tqdm(IDSample):
    article = reader.articleAt(UID2Position[ID])
    assert article["UID"] == ID
reader.close()
```

Reading all the entries:
```python
#Read all entries in order
batchSize = 1
# Change this to improve performance for certain
# access methods, such as remote mounts.

allDocTypes = set()
pbar = tqdm(total=74883967)

reader = WOS.DatabaseReader(WOSDataPath)

while True:
    articles = reader.readNextArticles(batchSize)
    if(articles):
        pbar.update(len(articles))
        for article in articles:
            if('doctype' in article):
                allDocTypes.update(set(article['doctype']))
    else:
        break
        
reader.close()
```



