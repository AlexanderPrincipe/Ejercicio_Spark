from pyspark import SparkContext
from itertools import islice
from pyspark.sql import SQLContext

sc = SparkContext("local", "_Ejercicio")
sqlContext = SQLContext(sc)
rdd_sin_cabeceras = sqlContext.read.csv('movie_metadata.csv', header=True).rdd



def mapCategories(x):
    if x[8] != "" and x[8] != None:
        return ( x[1], ( int(x[8]), 1 ))

# Directores que sus peliculas han tenido mas ingresos en promedio.

print("DIRECTORES CON MAYORES INGRESOS POR PELICULA")

directores = rdd_sin_cabeceras.filter(lambda x : x[1] != "" and x[8] != None) \
    .map(mapCategories) \
    .reduceByKey(lambda x,y : ( x[0] + y[0] , x[1] + y[1] )) \
    .map(lambda x : ( x[0] , x[1][0] / x[1][1]  ) ) \
    .sortBy(lambda x : x[1], ascending=False) \
    .take(5)

for d in directores:
    print(d)

# Peliculas que han tenido mejor margen

print("PELICULAS QUE MAS MARGEN TIENEN")

peliculas = rdd_sin_cabeceras.filter(lambda x : x[11]!=None  and x[22]!=None and x[8]!=None ) \
        .map(lambda x : (x[11] , int(x[8]) - int(x[22])  )) \
        .sortBy(lambda x : x[1], ascending=False) \
        .take(5)
for p in peliculas:
    print(p)

# Director con mejores criticas

print("DIRECTORES CON MEJORES CRITICAS")

directores = rdd_sin_cabeceras.filter(lambda x: x[1]!=None and x[25] is not None) \
        .map(lambda x : ( x[1] , ( float(x[25]) , 1)  )) \
        .reduceByKey(lambda x,y : ( x[0] + y[0], x[1] +y[1] )) \
        .map(lambda x : ( x[0], x[1][0] / x[1][1])) \
        .sortBy(lambda x : x[1], ascending=False) \
        .take(5)

for d in directores:
    print(d)



print("DIRECTORES CON MAS LIKES")

directores = rdd_sin_cabeceras.filter(lambda x: x[1]!=None and x[27] is not None) \
    .map(lambda x : ( x[1] , ( int(x[27]) , 1)  )) \
    .reduceByKey(lambda x,y : ( x[0] + y[0], x[1] +y[1] )) \
    .map(lambda x : ( x[0], x[1][0] / x[1][1])) \
    .sortBy(lambda x : x[1], ascending=False) \
    .take(5)

for d in directores:
    print(d)




print("ACTORES CON MAS LIKES")
actores = rdd_sin_cabeceras.filter(lambda x:x[10]!=None and x[7] is not None) \
    .map(lambda x : ( x[10] , ( int(x[7]) , 1)  )) \
    .reduceByKey(lambda x,y : ( x[0] + y[0], x[1] +y[1] )) \
    .map(lambda x : ( x[0], x[1][0] / x[1][1])) \
    .sortBy(lambda x : x[1], ascending=False) \
    .take(5)

for a in actores:
    print(a)
