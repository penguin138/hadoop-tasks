#!/usr/bin/python3
import sys

def normalize(word):
    newWord=""
    for letter in word:
        if letter.isalpha():
            newWord = newWord+letter
    return newWord.lower()


def map():
    for line in sys.stdin:
        wordsWithId = line.split('\t',1)
        articleId = wordsWithId[0]
        words = wordsWithId[1].split()
        for word in words:
            normalizedWord = normalize(word)
            if (len(normalizedWord)>0):
                print(normalizedWord + '\t' + articleId)

map()