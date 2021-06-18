# WOS
Simple tool to store and read dictionaries in bgz files.

# Installation
Clone this repository and use `pip install -e .` from that directory. You can also install it directly from the repository using `pip install git+https://github.com/filipinascimento/WOS.git -U`.

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
UID2IndexPath = dataPath / "WOSDataIndex_test.bgz"
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

#loading the reader
reader = WOS.DatabaseReader(WOSDataPath)

#reading articles in order (reading the first 2 articles)
articles = reader.readNextArticles(2)

# Each article include a "_position" attribute that can
# be used to retrieve the article
anArticle = articles[0]
print(anArticle["UID"], anArticle["_position"])

#random access can be accomplished by using the method articleAt
retrievedArticle = reader.articleAt(anArticle["_position"])
assert retrievedArticle["UID"] == anArticle["UID"]

reader.close()
```


Reading using the random access API and a index dictionary/file:
```python
# Example creating a index
reader = WOS.DatabaseReader(WOSDataPath)
maxCount = 100 #set to -1 to generate the index for all the the articles

indexDictionary = reader.generateIndex(showProgressbar=True, maxCount=maxCount)
for UID, position in indexDictionary.items():
    assert UID == reader.articleAt(position)["UID"]
    assert int(position) == int(reader.articleAt(position)["_position"])

indexFilePath = UID2IndexPath
reader.generateIndex(indicesPath=indexFilePath, showProgressbar=True, maxCount=maxCount)
indexDictionary = WOS.readIndicesDictionary(indexFilePath, showProgressbar=True, estimatedCount = maxCount)
for UID, position in indexDictionary.items():
    assert UID == reader.articleAt(position)["UID"]
    assert int(position) == int(reader.articleAt(position)["_position"])
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



