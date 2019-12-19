'''
Created on 2019年10月29日

@author: scott.Duan

'''
from py2neo import Graph, Node, Relationship
import cx_Oracle as ora

fileName = 'D:\\eclipse_scala\\workspace\\testPython\\recomendation\\data\\ml-1m\\movies.txt';
fileName = 'D:\\eclipse_scala\\workspace\\testPython\\recomendation\\data\\ml-1m\\users.txt';
fileName = 'D:\\eclipse_scala\\workspace\\testPython\\recomendation\\data\\ml-1m\\ratings.txt';

def loadTxt():
    with open(fileName, 'r') as file_to_read:
        while True:
            lines = file_to_read.readline()      # 整行读取数据        
            if not lines:
                break
            data = lines.split('::')
            print (data)
        
#         node = Node('Movie', movieid=data[0], title=data[1], geners=data[2])
#         node = Node('User', userid=data[0], gender=data[1], age=data[2], occupation=data[3], zipcode=data[4])
#         tx.create(node)

            r = Relationship(graph.nodes.match("User", userid=data[0]).first(), 'ratings',
                             graph.nodes.match("Movie", movieid=data[1]).first())
            r['rating'] = data[2]
            r['timestamp'] = data[3]
            
            tx.create(r)
            
def OraToNeo4jNode():
    conn = ora.connect('duanyang/123@192.168.1.114:1521/orcl')
    c = conn.cursor()
    sql_str = "select * from occupation";
    sql_str = "select * from movie_type";
    x = c.execute(sql_str)
    while (1):
        rs = x.fetchone()
        if rs == None: 
            break
        print (rs[0])    
#         node = Node('Occupation', occupationid=rs[0], name=rs[1])
        node = Node('movie_type', type=rs[0])
        tx.create(node)
        
    c.close()
    conn.close()
    
def OraToNeo4jRelation():
    conn = ora.connect('duanyang/123@192.168.1.114:1521/orcl')
    c = conn.cursor()
    sql_str = "select * from users";
    x = c.execute(sql_str)
    while (1):
        rs = x.fetchone()
        if rs == None: 
            break
        
        print (graph.nodes.match("User", userid=str(rs[0])).first())
        print (graph.nodes.match("Occupation", occupationid=rs[3]).first())  
          
        r = Relationship(graph.nodes.match("User", userid=str(rs[0])).first(), 
                         'occupation_is',
                         graph.nodes.match("Occupation", occupationid=rs[3]).first())
        tx.create(r)
        
    c.close()
    conn.close()    
    
def RelationMovieType():
    conn = ora.connect('duanyang/123@192.168.1.114:1521/orcl')
    c = conn.cursor()
    sql_str = "select * from movies";
    x = c.execute(sql_str)
    while (1):
        rs = x.fetchone()
        if rs == None: 
            break
        
        movie = graph.nodes.match("Movie", movieid=str(rs[0])).first()
        if movie == None:
            print(rs[0])
        
        genres = str(rs[2].replace("\n", '')).split("|")
        print (genres)
        for i in genres:
#             print (i)
            movie_type = graph.nodes.match("movie_type", type=i).first()
            if movie_type == None:
                print (i)     
         
            r = Relationship(movie, 
                             'movie_type_is',
                             movie_type)
            tx.create(r)
        
    c.close()
    conn.close()    

if __name__ == '__main__':
    graph = Graph('http://192.168.2.201:7474/', username='neo4j', password='123')
    tx = graph.begin(False)  
    
#     loadTxt()    
#     OraToNeo4jNode()
#     OraToNeo4jRelation()    
    RelationMovieType()
    
    tx.commit()
    print(graph.nodes.match("User", userid=1).first())
    