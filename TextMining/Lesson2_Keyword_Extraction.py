#################################################################################################################
#################################### Lesson2 keyword Extraction #################################################
#############################################################################################################
import pandas as pd

""" In this lesson, we will learn how to extract key word from articles, since they provide a concise representation
    of the article’s content. Keywords also play a crucial role in locating the article from information retrieval
    systems, bibliographic databases and for search engine optimization. Keywords also help to categorize the article
    into the relevant subject or discipline.

    We will use NLP techniques on a collection of articles to extract keywords
    """


""" About the dataset 

The dataset which we use is from Kaggle (https://www.kaggle.com/benhamner/nips-papers/home). Neural Information 
Processing Systems (NIPS) is one of the top machine learning conferences in the world. This dataset includes the 
title and abstracts for all NIPS papers to date (ranging from the first 1987 conference to the current 2016 conference).

The nips-papers.csv contains the following columns:
- id,
- year,
- title,
- event_type,
- pdf_name : the name of the pdf file
- abstract : abstract of the paper
- paper_text : paper main content body

In this lesson, we focus on the concept of keyword extraction, so we only use abstracts of these articles to extract
keywords, because full text search is time consuming
"""

################################# 2.0 Key stages #########################################

"""
1. Text pre-processing
   a. noise removal
   b. normalisation

2. Data Exploration
   a. Word cloud to understand the frequently used words
   b. Top 20 single words, bi-grams and tri grams
   
3. Convert text to a vector of word counts

4. Convert text to a vector of term frequencies

5. Sort terms in descending order based on term frequencies to identify top N keywords
"""

########################## 2.1 Text pre-processing #########################################

############# 2.1.1 Load the dataset ################
df=pd.read_csv('/DATA/data_set/spark/pyspark/Lesson2_Keyword_Extraction/nips-papers.csv', delimiter=',',error_bad_lines=False)
abstractDf=df[['id','year','title','abstract']]

abstractDf.to_csv('/tmp/abstract1.csv',sep='|', header=True, index=False)
#print(heads)

############ 2.1.2 Fetch word count for each abstract #############################

