#!/usr/bin/python3
import sys

def getTfs(articleIds):
    articleIds.sort()
    prevId = ""
    tf = 0
    firstTime = True
    idsWithTfs = []
    for id in articleIds:
        if (firstTime):
            prevId = id
            firstTime = False
        if id == prevId:
            tf+=1
        else:
            idsWithTfs.append((prevId,tf))
            tf = 1
        prevId = id
    idsWithTfs.append((prevId,tf))
    return idsWithTfs

def getTfsSmart(articleIds):
    myDictionary = dict()
    idsWithTfs=[]
    for id in articleIds:
        if id in myDictionary:
            myDictionary[id]+=1
        else:
            myDictionary[id]=1
    for key in myDictionary.keys():
        idsWithTfs.append((key,myDictionary[key]))
    return idsWithTfs


def reduce():
    currentTerm = ""
    prevTerm = ""
    articleIds = []
    firstTime = True
    for line in sys.stdin:
        termWithIds = line.strip().split('\t',1)
        currentTerm = termWithIds[0]
        if (firstTime):
            prevTerm = currentTerm
            firstTime = False
        if currentTerm == prevTerm:
            #print(termWithIds)
            articleIds.append(termWithIds[1])
        else:
            idsWithTfs = getTfsSmart(articleIds)
            #print(idsWithTfs)
            print(prevTerm, end = "\t")
            for pair in idsWithTfs:
                print('('+pair[0]+',',str(pair[1])+')',end=" ")
            print('\n')
            articleIds = [termWithIds[1]]
        prevTerm = currentTerm
    idsWithTfs = getTfsSmart(articleIds)
    print(prevTerm, end = "\t")
    for pair in idsWithTfs:
        print('('+pair[0]+',',str(pair[1])+')',end=" ")
    print('\n')

reduce()