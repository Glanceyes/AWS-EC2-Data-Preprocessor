# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.13.6
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

from konlpy.tag import Okt
from krwordrank.word import KRWordRank 
from krwordrank.word import summarize_with_keywords


class NounExtractor:
    def __init__(self):
        self.okt = Okt()
    
    def extractNoun(self, text):
        try: 
            nounList = self.okt.nouns(text)
            return nounList
        except TypeError as e:
            print(e)


class WordRankExtractor:
    def __init__(self, minCount = 1, maxLength = 15):
        self.wordRankExtractor = KRWordRank(min_count = minCount, max_length = maxLength)
    
    def extractWordRank(self, texts, beta = 0.85, maxIterNum = 10):
        keywords, rank, graph = self.wordRankExtractor.extract(texts, beta = beta, max_iter = maxIterNum)
        word, r = sorted(keywords.items(), key=lambda x:x[1], reverse=True)[0]
        return word


class KeywordExtractor:
    nounExtractorInstance = NounExtractor()
    wordRankExtractorInstance = WordRankExtractor()
    
    def __init__(self, minCount = 1, maxLength = 15):
        self.nounExtractorInstance = NounExtractor()
        self.wordRankExtractorInstance = WordRankExtractor()
    
    def extractKeyword(self, inputText, nounNum = 2, beta = 0.85, maxIterNum = 10):
        nounList = self.nounExtractorInstance.extractNoun(inputText)

        # 명사가 nounNum개 이상 추출된 경우에만 Rank를 구한다.
        if (len(nounList) < nounNum):
            raise

        text = ' '.join(nounList)

        try:
            word = self.wordRankExtractorInstance.extractWordRank([text], beta, maxIterNum)
            return word
        except:
            raise
            
    def extractKeywordFromTexts(self, inputTexts, nounNum = 2, beta = 0.85, maxIterNum = 10):
        nounPerSentence = list()
        
        for sentence in inputTexts:
            nounList = self.nounExtractorInstance.extractNoun(sentence)
            # 명사가 nounNum개 이상 추출된 경우에만 Rank를 구한다.
            if (len(nounList) < nounNum):
                continue

            text = ' '.join(nounList)
            nounPerSentence.append(text)
            
        try:
            word = self.wordRankExtractorInstance.extractWordRank(nounPerSentence, beta, maxIterNum)
            return word
        except:
            raise

# + active=""
# # Only for test
#
# keywordExtractorInstance = KeywordExtractor()
# inputTexts = ["안녕하세요?", "반갑습니다", "저는 대한민국 서울에 삽니다.", "나이는 밝히지 않고 싶어요."]
# keyword = keywordExtractorInstance.extractKeywordFromTexts(inputTexts)
# print(keyword)
