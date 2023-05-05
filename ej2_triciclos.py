"""
Fermín González Pereiro
"""
"""
Considera que los datos, es decir, la lista de las aristas del grafo, no se encuentran en un único fichero
sino en muchos, los cuales pasamos como entrada.

Calcular los 3 ciclos del grafo teniendo en cuenta los múltiples ficheros.
"""
import sys
from pyspark import SparkContext
sc = SparkContext()


def mapper(line):
    edge = line.strip().split(',')
    n1 = edge[0]
    n2 = edge[1]
    if n1 < n2:
         return (n1,n2)
    elif n1 > n2:
         return (n2,n1)
    else:
        pass #n1 == n2       

def relation(tupla):
    triciclos=[]
    for i in range(len(tupla[1])):
        triciclos.append(((tupla[0],tupla[1][i]),'exists'))
        for j in range(i+1, len(tupla[1])):
                triciclos.append(((tupla[1][i],tupla[1][j]),('pending',tupla[0])))
    return triciclos

def lista_ternas(triciclos):
    l_triciclos=[]
    for t, info in triciclos.collect():
        info2=list(info)
        if len(info2)>1 and 'exists' in info2:
            for pos in info2:
                if pos!='exists': 
                    l_triciclos.append((pos[1],t[0],t[1]))
    return l_triciclos

def main(sc, filenames):
    graph=sc.emptyRDD()
    for file in filenames:
        graph = graph.union(sc.textFile(file))
    graph_clean=graph.map(mapper).distinct().filter(lambda x: x!=None)
    adyacents=graph_clean.groupByKey().map(lambda x: (x[0],sorted(list(x[1])))).sortByKey()
    triciclos=adyacents.flatMap(relation).groupByKey()
    lista_triciclos=lista_ternas(triciclos)
    print("-----RESULTADOS-----")
    print("Los triciclos del grafo son: ", sorted(lista_triciclos))
    
if __name__ == "__main__":
    filenames = []
    if len(sys.argv) <= 2: #hemos de meter al menos 2 ficheros
        print(f"Uso: python3 {0} <file>")
    else:
        for i in range(len(sys.argv)):
            if i != 0:
                filenames.append(sys.argv[i]) #del argumento 1 en adelante son los archivos, asi que los metemos en la lista de trabajo.
        main(sc,filenames)