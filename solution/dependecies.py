from os import listdir
from os.path import isfile, join,basename
import re
import networkx as nx
import matplotlib.pyplot as plt
from threading import Thread, Lock

fig = plt.figure(figsize=(12,12))
ax = plt.subplot(111)
ax.set_title('SQL table dependencies', fontsize=10)

tmpDir=r"./tmp"
productDict={}
rawproductDict={}
onlyfiles = [join(tmpDir,f) for f in listdir(tmpDir) if isfile(join(tmpDir, f))]

#print(dict_relation)
def invert(d):
    '''Invert dictionary'''
    i = {}
    for k,v in d.items():
        if isinstance(v, (list,)):
            for table in v:
                tablel = i.get(table, [])
                tablel.append(k)
                i[table] = tablel
        else:
            tablel = i.get(v, [])
            tablel.append(k)
            i[v] = tablel
    return i

def order(iproduct, val=None, level=0):
    '''Generates a relative order in product dictionary'''
    results = {}
    if val is None:
        for k,v in iproduct.items():
            for table in v:
                results.setdefault(k,0)
                d = order(iproduct, val=table, level=level+1)
                for dk, dv in d.items():
                    if dv > results.get(dk,0):
                        results[dk] = dv
        return results
    else:
        results[val] = level
        tables = iproduct.get(val, None)
        if tables is None or tables == []:
            return { val: level }
        else:
            for table in tables:
                d = order(iproduct, val=table, level=level+1)
                for dk, dv in d.items():
                    if dv > results.get(dk,0):
                        results[dk] = dv
            return results

def flatten(productDict):
    '''flatten() generates a list of sqlfiles in order'''
    iproduct = invert(productDict)
    orderProduct = order(iproduct)
    iorder = invert(orderProduct)

    #Sort the keys and append to a list
    output = [] 
    for key in sorted(iorder.keys()):
        output.extend(iorder[key])
    return output,iorder

def plotting(iproduct):
    g=nx.Graph()
    for k, v in iproduct.items():
        if isinstance(v, (list,)):
            for table in v:
                g.add_edge(k,table)
        elif v is None or v == []:
            g.add_node(k)
   
    print("plotting")
    nx.draw(g, with_labels = True)
    plt.tight_layout()
    #plt.show()
    plt.savefig("Graph.png", format="PNG")


def run(query,result):
    try:
        conn = connect(host=host, port=port, database=db)
        curs = conn.cursor()
        curs.execute(query)
        result = curs
        curs.close()
        conn.close()
    except Exception as e:
            raise("Unable to access database %s" % str(e))
   

def thread(v):
    threads = []
    for i in range(len(v)):
        script=v[i]
        f = open(script)
        full_sql = f.read() 
        query = full_sql.split(';') 
        process = Thread(target=run,args=query)
        process.start()
        threads.append(process)
    for process in threads:
        process.join()

for SQLfile in onlyfiles:
    with open(SQLfile) as f:
        filename='tmp.'+basename(SQLfile).split('.')[0]
        lines = f.read().splitlines()
        sqlquery=' '.join(lines)
    mylist=[]
    #searching all dependecies (FROM and JOIN clause) and store it in dictionary
    table = re.search('FROM \s*\`(\w+.\w+[_]*)\`.*',sqlquery).groups(0)[0]
    mylist.append(table)
    regex = r"JOIN \`(\w+.\w+[_]*)\s*"
    relation = re.findall(regex,sqlquery)
    mylist.extend(relation)
    rawproductDict[filename]=mylist
    productDict[filename]=[ x for x in mylist if "raw." not in x ]
#plotting graph that show relationship with networkx lib
plotting(rawproductDict)
#ordering sql scripts in correct order
sqlOrder,iorder = flatten(productDict) 
print(sqlOrder)
print(">>>>>>>>>>>>><<<<<")
print(iorder)

for k in sorted(iorder.keys()):
    v=iorder[k]
    thread(v)


        
        