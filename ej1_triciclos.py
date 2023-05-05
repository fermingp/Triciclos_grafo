"""
Fermín González Pereiro
"""

"""
Calculamos los triciclos que hay en el grafo cuyo contenido viene en un archivo de texto
el cual contiene en sus líneas las aristas del grafo
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

"""
Los 3-ciclos muestran una relación estrecha entre 3 entidades (vértices): cada uno
de los vértices está relacionado (tiene aristas) con los otros dos
"""
    
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

def main(sc, filename): 
    graph = sc.textFile(filename)
    graph_clean = graph.map(mapper).distinct().filter(lambda x: x != None)
    adyacents=graph_clean.groupByKey().map(lambda x: (x[0],sorted(list(x[1])))).sortByKey()
    triciclos=adyacents.flatMap(relation).groupByKey()
    lista_triciclos=lista_ternas(triciclos)
    print("-----RESULTADOS-----")
    print("Los triciclos del grafo son: ", sorted(lista_triciclos))
    
    
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Uso: python3 {0} <file>")
    else:
        main(sc,sys.argv[1])

