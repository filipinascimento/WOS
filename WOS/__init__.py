#!/usr/bin/python
import ujson
from . import bgzf
import struct
from tqdm.auto import tqdm

coders = {
    "json": ujson,
}

class DatabaseWriter:
    def __init__(self,filename, coderName = "json", wosOpen=bgzf.open):
        self.fileHandler = wosOpen(filename,"wb")
        self.coderName = coderName
        self.coder = coders[coderName]
        self.fileHandler.write(("%s\n"%coderName))

    def writeArticles(self,articles):
        fd = self.fileHandler
        coder = self.coder
        positions = []
        for article in articles:
            positions.append(fd.tell())
            data = (coder.dumps(article))
            length = len(data) 
            fd.write(struct.pack("<Q",length))
            fd.write(data)
        return positions

    def close(self):
        self.fileHandler.close()


class DatabaseReader:
    def __init__(self,filename, wosOpen=bgzf.open):
        self.fileHandler = wosOpen(filename,"rb")
        self.coderName = self.fileHandler.readline().decode("utf8").strip()
        self.coder = coders[self.coderName]
        self.startPosition = self.fileHandler.tell()

    def readNextArticles(self,maxArticlesCount = -1,showProgressbar = False):
        if(showProgressbar):
            pbar = tqdm(total=maxArticlesCount)
        fd = self.fileHandler
        coder = self.coder
        readCount = 0
        articles = []
        while (maxArticlesCount==-1 or readCount<maxArticlesCount):
            if(showProgressbar):
                pbar.update(1)
            position = fd.tell()
            sizeData = fd.read(8)
            if(len(sizeData)==0):
                break #end of file
            else:
                size = struct.unpack("<Q",sizeData)[0]
                data = fd.read(size)
                article = coder.loads(data)
                article["_position"] = str(position)
                articles.append(article)
                readCount+=1
        if(showProgressbar):
            pbar.close()
        return articles
    
    def reset(self):
        self.fileHandler.seek(self.startPosition)

    def articleAt(self,position):
        fd = self.fileHandler
        coder = self.coder
        fd.seek(int(position))
        sizeData = fd.read(8)
        size = struct.unpack("<Q",sizeData)[0]
        data = fd.read(size)
        article = coder.loads(data)
        article["_position"] = str(position)
        return article

    def currentPosition(self):
        return self.fileHandler.tell()
    
    #   def recoverPositions(self):
    #     fd = self.fileHandler
    #     positions.append(fd.tell())
    #     this.readNextArticles(1)
    #     return positions
    
    def close(self):
        self.fileHandler.close()

    # def buildIndex(IDs,positions,indicesDataPath):
    #     with bgzf.open(indicesDataPath,"w") as fd:
    #         for i in tqdm(range(len(positions))):
    #             data = IDs[i].encode("utf8")
    #             fd.write(struct.pack("<QQ",len(data)+8,positions[i])+data)

    def generateIndex(self,indicesPath=None, showProgressbar = False, maxCount = -1, estimatedCount=74883966):
        if(showProgressbar):
            if(maxCount>=0):
                estimatedCount = maxCount
            pbar = tqdm(total=estimatedCount)
        savedPosition = self.currentPosition()
        self.reset()
        if(indicesPath is not None):
            fd = bgzf.open(indicesPath,"w")
        else:
            indexDictionary = {}
        articlesCount = 0
        while True:
            articles = self.readNextArticles(100)
            if(articles):
                for article in articles:
                    articlesCount+=1
                    if(maxCount>=0 and articlesCount>maxCount):
                        break
                    if(showProgressbar):
                        pbar.update(1)
                    else:
                        if('UID' in article):
                            UID = article["UID"]
                            position = int(article["_position"])
                            if(indicesPath is not None):
                                data = UID.encode("utf8")
                                fd.write(struct.pack("<QQ",len(data)+8,position)+data)
                            else:
                                indexDictionary[UID] = position
            else:
                break
            if(maxCount>=0 and articlesCount>maxCount):
                break
        self.fileHandler.seek(savedPosition)

        if(showProgressbar):
            pbar.close()
        if(indicesPath is not None):
            fd.close()
            return
        else:
            return indexDictionary

def writeWOSDataBase(inputJSONPathsList,databaseDataPath):
    writer = DatabaseWriter(databaseDataPath)
    IDs = []
    positions = []
    wosFiles = inputJSONPathsList
    for filename in tqdm(wosFiles):
        WOSData = []
        with open(filename,"rt") as fd:
            for line in fd:
                WOSData.append(ujson.loads(line))
                IDs.append(WOSData[-1]["UID"])
        positions.extend(writer.writeArticles(WOSData))
    writer.close()
    return IDs,positions
    
    


            
def readIndicesDictionary(indicesDataPath,showProgressbar=False,estimatedCount=74883966):
    with bgzf.open(indicesDataPath,"rb") as fd:
        if(showProgressbar):
            pbar = tqdm(total=estimatedCount)
        UID2Positions = {}
        while True:
            if(showProgressbar):
                pbar.update(1)
            data = fd.read(8*2)
            if(len(data)==8*2):
                dataSize,position = struct.unpack("<QQ",data)
                newID = fd.read(dataSize-8).decode("utf-8")
                UID2Positions[newID] = position
            else: 
                break
        if(showProgressbar):
            pbar.close()
        return UID2Positions
