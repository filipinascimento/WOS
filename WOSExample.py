# %%
import WOS
from pathlib import Path
from tqdm.auto import tqdm
import importlib
importlib.reload(WOS)


# %%
# dataPath = Path("..") / ".." / "Data" / "Processed"
dataPath = Path("/gpfs/sciencegenome/WoS-disambiguation/")
WOSDataPath = dataPath / "WOSData.bgz"
UID2IndexPath = dataPath / "WOSDataIndex_test.bgz"


# %%
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


# %%
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


# %%
#Read all entries in order
# and get all the doctypes
allDocTypes = set()
pbar = tqdm(total=74883967)

reader = WOS.DatabaseReader(WOSDataPath)

while True:
    articles = reader.readNextArticles(1) #change this to 
    if(articles):
        pbar.update(len(articles))
        for article in articles:
            if('doctype' in article):
                allDocTypes.update(set(article['doctype']))
    else:
        break
        
reader.close()



# %%
