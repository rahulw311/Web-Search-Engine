import webbrowser
from flask import Flask, render_template, request, redirect
import mysql.connector
from linkpreview import link_preview
import math
from py4j.java_gateway import JavaGateway
import time
import pickle 
import os


app = Flask(__name__)
conn = mysql.connector.connect(user='root', password='root', host='localhost', database='irproject')
cursor = conn.cursor()
htmlstart='''<!DOCTYPE html>
<html>
  <head>
    <title>Top Results</title>
    <link href="//maxcdn.bootstrapcdn.com/bootstrap/4.1.1/css/bootstrap.min.css" rel="stylesheet" id="bootstrap-css">
    <script src="//maxcdn.bootstrapcdn.com/bootstrap/4.1.1/js/bootstrap.min.js"></script>
    <script src="//cdnjs.cloudflare.com/ajax/libs/jquery/3.2.1/jquery.min.js"></script>
    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css" integrity="sha384-MCw98/SFnGE8fJT3GXwEOngsV7Zt27NXFoaoApmYm81iuXoPkFOJwJ8ERdknLPMO" crossorigin="anonymous">
    <link rel="stylesheet" href="https://use.fontawesome.com/releases/v5.5.0/css/all.css" integrity="sha384-B4dIYHKNBt8Bc12p+WXckhzcICo0wtJAoU8YZTY5qE0Id1GSseTk6S+L3BlXeVIU" crossorigin="anonymous">
    <style>


    body,html{
    height: 100%;
    width: 100%;
    background: white;
    }

    .wrapper{
      align-items: center;
    }

    .heading{
      margin-top: 5%;
      font-size: 20px;
      font-family: sans-serif;
    }
    </style>
  
  </head>
  <body>
'''
htmlend='''</body>
</html>
'''


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        userDetails = request.form
        query = userDetails['query']
        print(query)
        choice= userDetails['rankchoice']
        if(choice=="0"):
            lucene(query)
        else:
            hadoop(query)
    return render_template('search.html')

def hadoop(query):
    cursor = conn.cursor()
    resfile=open("results.html","w")
    resfile.write(htmlstart)
    resfile.write("<h3>Top results for: "+query+" with Hadoop</h3>")
    ranks=ranker(query.split(' '))
    for i in range(10):
        resfile.write("<p>")
        file=ranks[i][0]
        print(file)
        score=ranks[i][1]
        print(score)
        cursor.execute("select title from crawler_data where file=%s",([file]))
        for i in cursor:
            title=i
        titlep=title[0]
        cursor.execute("select url from crawler_data where file=%s",([file]))
        for i in cursor:
            url=i
        urlp=url[0]
        resfile.write("<a href="+urlp+">"+titlep+"</a>(Score: "+str(score)+")")
        desc=snippet(urlp)
        if(desc is None):
            pass
        else:
            resfile.write("<br>Description: "+desc)
        resfile.write("</p>")
    resfile.write(htmlend)
    resfile.close()
    webbrowser.open("results.html")


def lucene(query):
    resfile=open("results.html","w")
    gateway=JavaGateway()
    LuceneSearcher=gateway.entry_point
    resultList=LuceneSearcher.LuceneSearch(query)
    resultListLen=len(resultList)

    resfile.write(htmlstart)
    resfile.write("<h3>Top results for: "+query+" with Lucene</h3>")
    for i in range(0,resultListLen):
        resfile.write("<p>")
        resfile.write("<a href="+resultList[i][3]+">"+resultList[i][4]+"</a>(Score: "+str(resultList[i][1])+")")
        desc=snippet(str(resultList[i][3]))
        if(desc is None):
            pass
        else:
            resfile.write("<br>Description: "+desc)
        resfile.write("</p>")
    resfile.write(htmlend)
    resfile.close()
    webbrowser.open("results.html")

def snippet(url):
    preview=link_preview(url)
    desc=preview.description
    return desc


def retrievedata(word):
    cursor.execute("select * from irdb where word=%s",([word]))
    row = 0
    for i in cursor:
        row+=1
        return i
    


def ranker(query):
   
    with open('lengths.pickle', 'rb') as handle:
        lengths = pickle.load(handle)
   
    ranks = {}
    #print(query)
    documents_to_rank = {}
    corpus_counts = {}
    doc_counts = {}
    words_counts = {}
    query_freq = {}
    for word in query:
        query_freq[word] = query_freq.get(word,0)+1
        word_counts = {}
        row = retrievedata(word)
        corpus_counts[row[0]] = int(row[2])
        doc_counts[row[0]] = int(row[3])
        for pair in row[1].split('_'):
            temp_data = pair.split(':')
            doc_id = temp_data[0].lstrip()
            count_in_docid = int(temp_data[1])
            documents_to_rank[doc_id] = False
            word_counts[doc_id] = count_in_docid

        words_counts[word] = word_counts
   
    #print(len(documents_to_rank.keys()))
    #print(doc_counts)
    #print(words_counts)
    #Calculating Ranks
    #constants
    k1=1.2
    k2=100
    b=0.75
    #k=1.11
    #no of docs - change later
    N = 16000
    avgdl = 2804
    for docid in documents_to_rank.keys():
        #get_variables
        doc_score = 0
        k = k1*((1-b)+((b)*(lengths[docid]/avgdl)))
        for word in query_freq.keys():
            ni = doc_counts[word]
            fi = words_counts[word].get(docid,0)
            qfi = query_freq[word]
            term1 = math.log(1/((ni + 0.5)/(N-ni+0.5)))
            term2 = ((k1+1)*(fi))/(k+fi)
            term3 = ((k2+1)*(qfi))/(k2+qfi)
            final_term = term1*term2*term3
            doc_score+=final_term
       
        ranks[docid] = doc_score
    sorted_ranks = [(k,v) for k, v in sorted(ranks.items(), key=lambda item: item[1])]
    top_results = sorted_ranks[::-1]
    return top_results[:20]


if __name__ == '__main__':
    app.run(debug=True)
