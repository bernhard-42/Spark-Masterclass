
# (LAB 1) Preparation

**Note:** Run this before any spark command in the notebook. Restart interpreter if necessary!



```scala
%dep
z.reset()
z.addRepo("Spark Packages Repo").url("http://dl.bintray.com/spark-packages/maven")
z.load("com.databricks:spark-csv_2.10:1.3.0")

```


Calling sc will initialize the executors (org.apache.spark.executor.CoarseGrainedExecutorBackend) via yarn, if Zeppelin is configured as "yarn-client"


```python
%pyspark

print(sc.version)
```


# (LAB 2) Working with RDDs


```python
%pyspark

def pprint(array):
    for a in array:
        print a
```


## 2.1 Create a simple RDD and sum up rows 


```python
%pyspark

import random

data = [ [random.randint(10,99) for col in range(4)] for row in range(10)]

rdd = sc.parallelize(data, 4)

pprint(rdd.collect())
```


```python
%pyspark

m = rdd.map(lambda x: sum(x))
print(m.collect())

s = m.reduce(lambda x,y: x + y)
print "total = ", s

```


## 2.2 Load the famous Iris data from HDFS and so some basic calculations

Attribute Information:

    [0] sepal length in cm
    [1] sepal width in cm
    [2] petal length in cm
    [3] petal width in cm
    [4] class: Iris Setosa, Iris Versicolour, Iris Virginica



```python
%pyspark

def split(row):
    parts = row.split(",")
    return [float(v) for v in parts[:4]] + [parts[4]]
    
file = sc.textFile("/tmp/iris.data")

# remove empty lines and split each line
iris = file.filter(lambda row: len(row)>0)\
           .map(split)

print iris.count()
print 
pprint(iris.sample(False, fraction=0.1, seed=42).take(15))
```


Calculate average sepal length per class


```python
%pyspark

tuples = iris.map(lambda row: [row[4], row[0]]) 

result = tuples.groupByKey().mapValues(lambda row: sum(row)/len(row))

pprint(result.collect())
```


# (LAB 3) Working with DataFrames converted from RDDs


## 3.1 Transform Iris RDD to DataFrame


```python
%pyspark

from pyspark.sql.types import *

schema = StructType([ \
   StructField("sepalLength", DoubleType(),  True), \
   StructField("sepalWidth",  DoubleType(),  True), \
   StructField("PetalLength", DoubleType(),  True), \
   StructField("PetalWidth",  DoubleType(),  True), \
   StructField("Class",       StringType(),  True)
])

irisDf = sqlContext.createDataFrame(iris, schema=schema)

sqlContext.registerDataFrameAsTable(irisDf, "Iris")

irisDf.show()
```


```python
%pyspark

irisDf.select(["Class", "sepalLength"]).groupBy("Class").avg("sepalLength").show()
```


```sql
%sql

select Class, avg(sepalLength) as avgSL
from Iris
group by Class
```


# (LAB 4) Analyze World Development Indicators 

## 4.1 Load World Development Indicators from HDFS as DataFrame

**Data source:** https://www.kaggle.com/worldbank/world-development-indicators/downloads/world-development-indicators-release-2016-01-28-06-31-53.zip

**Changes:** Reduced to a subset with countries of the European Union only  (AUT, BEL, BUL, CYP, CZE, DEU, DNK, ESP, EST, FIN, FRA, GBR, GRC, HRV, HUN, IRL, ITA, LTU, LUX, LVA, MLT, NLD, POL, PRT, ROM, SVK, SVN, SWE) 


```bash
%sh

hdfs dfs -ls /tmp/europe-indicators.csv
```


```python
%pyspark
from pyspark.sql.types import *

schema = StructType([ \
   StructField("CountryName",   StringType(),  True), \
   StructField("CountryCode",   StringType(),  True), \
   StructField("IndicatorName", StringType(),  True), \
   StructField("IndicatorCode", StringType(),  True), \
   StructField("Year",          IntegerType(), True), \
   StructField("Value",         DoubleType(),  True)  \
])

indicators_csv = sqlContext.read.load('/tmp/europe-indicators.csv', format='com.databricks.spark.csv', header='true', schema=schema).cache()
sqlContext.registerDataFrameAsTable(data, "IndicatorsRDD")

print(indicators_csv.count())


```


Let's look at the schema of the Indicators table


```python
%pyspark
indicators_csv.printSchema()
indicators_csv.show()
```


Code/value encoding is not that optimal ... Let's transform the data set and store the result os ORC



## 4.2 Transform Indicators table to Columns 



Spark 1.5 does not provide a `pivot` method for DataFrames, hence we need to write our own pivot via RDDs and `aggregateByKey`

Some caveats for this step:
- Return a row from `merge`, python dictionaries are deprecated
- `**value` is a nice trick to convert a dictionary to a keyword parameter list (Rows are unmutable)
- Initialize with all indicators and set them to None
- `.`are not allowed in column names, so replace with `_`


```python
%pyspark

columns = indicators_csv.map(lambda row: row.IndicatorCode.replace(".", "_")).distinct().collect()
bc = sc.broadcast(columns)

def seq(u, v):
    if u == None: 
        u = {ind: None for ind in bc.value}          # Use value of broadcast variable to initialize the dictionary and ensure all rows have all indicators
    u[v.IndicatorCode.replace(".","_")] = v.Value    # Set this indicators value converted to float
    return u

def comb(u1, u2):
    u1.update(u2)
    return u1

def merge(keys, value):
    value["Country"] = keys[0]
    value["Year"] = int(keys[1])
    return Row(**value)

data = indicators_csv.select(["CountryCode", "IndicatorCode", "Year", "Value"])\
                     .rdd\
                     .keyBy(lambda row: row.CountryCode + "|" + str(row.Year))\
                     .aggregateByKey(None, seq, comb)\
                     .map(lambda tuple: merge(tuple[0].split("|"), tuple[1]))\
                     .cache()


```


Finally, transform RDD back to DataFrame and register a table with the hiveContext (due to ORC)

**Notes:**

- The StructType schema **has to be sorted!** Spark does not match schema names with Row column names but uses the order of elements in Row and schema to apply types
- Also, due to the many null values, automatic schema inference will only work properly when "samplingRatio=100" in createDataFrame. However, I wouldn't rely on it ...


```python
%pyspark

from pyspark.sql.types import *

sqlContext.setConf("spark.sql.orc.filterPushdown", "true")

fields = [StructField(ind, DoubleType(), True) for ind in columns ] + \
         [StructField("Year", IntegerType(), False), StructField("Country", StringType(), False)]
sortedFields = sorted(fields, key=lambda x: x.name)
sortedSchema = StructType(fields=sortedFields)

indicators = sqlContext.createDataFrame(data, schema = sortedSchema)
sqlContext.registerDataFrameAsTable(indicators, "Indicators")
```


## 4.3 Save transformed table as ORC


```bash
%sh
hdfs dfs -rm -r /tmp/europe-indicators_transformed_orc

```


```python
%pyspark

indicators.write.orc("/tmp/europe-indicators_transformed_orc")

```


## 4.4 Some simple Queries


Load ORC data again to benefit from predicate pushdow, etc


```python
%pyspark

indicators_t = sqlContext.read.orc("/tmp/europe-indicators_transformed_orc")
sqlContext.registerDataFrameAsTable(indicators_t, "Indicators_t")
sqlContext.cacheTable("Indicators_t")

```


Execute some queries


```sql
%sql

-- SP.DYN.CBRT.IN: Birth rate, crude (per 1,000 people)

select Country, Year, SP_DYN_CBRT_IN from Indicators_t
where Country in ('AUT', 'FRA', 'DEU', 'GRC', 'IRL', 'ITA', 'NLD', 'PRT', 'ESP', 'GBR') 
  and Year > 1990
order by Country, Year

```


```sql
%sql

-- SL.UEM.1524.NE.ZS: Unemployment, youth total (% of total labor force ages 15-24) (national estimate)

select Country, Year, SL_UEM_1524_NE_ZS from Indicators_t
where Country in ('AUT', 'FRA', 'DEU', 'GRC', 'IRL', 'ITA', 'NLD', 'PRT', 'ESP', 'GBR') 
  and Year > 1990
order by Country, Year


```


```sql
%sql

-- SL.UEM.1524.NE.ZS: Unemployment, youth total (% of total labor force ages 15-24) (national estimate)
-- SL.UEM.TOTL.NE.ZS: Unemployment, total (% of total labor force) (national estimate)
-- SP.DYN.CBRT.IN: Birth rate, crude (per 1,000 people)

select Country, Year, SL_UEM_1524_NE_ZS, SP_DYN_CBRT_IN  from Indicators_t
where Country in ('AUT', 'FRA', 'DEU', 'GRC', 'IRL', 'ITA', 'NLD', 'PRT', 'ESP', 'GBR')
  and Year > 1990
  and Year < 2015
order by Country, Year

```


## Optional: Pure SQL approach

Of course, this result could have been calculated without pivoting the table


```python
%pyspark

sqlContext.registerDataFrameAsTable(indicators_csv, "Indicators")
```


```sql
%sql

select Year, CountryCode, max(SL) as UNEM, max(SP) as CBRT from
  (select Year, CountryCode, 
          case IndicatorCode when 'SP.DYN.CBRT.IN'  then max(Value) else NULL end as SP,
          case IndicatorCode when 'SL.UEM.1524.NE.ZS' then max(Value) else NULL end as SL
   from Indicators
   where IndicatorCode in ('SP.DYN.CBRT.IN', 'SL.UEM.1524.NE.ZS') 
     and CountryCode in ('AUT', 'FRA', 'DEU', 'GRC', 'IRL', 'ITA', 'NLD', 'PRT', 'ESP', 'GBR') 
     and year > 1990
   group by Year, CountryCode, IndicatorCode
   order by Year, CountryCode
  ) Indicators2
group by CountryCode, Year
```


# (Lab 5) Clustering with Spark ML

## 5.1 Select relevant data



```python
%pyspark

def cvtCodes(code):
    return code.lower().replace(".", "_")
    
#euCodes = [
#    "BEL","GRC","MLT","SVK","BUL","IRL","NLD",
#    "SVN","DNK","ITA","AUT","ESP","DEU","HRV",
#    "POL","CZE","EST","LVA","PRT","HUN","FIN",
#    "LTU","ROM","GBR","FRA","LUX","SWE","CYP"
#]

features = [
    cvtCodes(c) for c in [
        "SL.UEM.1524.NE.ZS",   # Unemployment, youth total (% of total labor force ages 15-24) (national estimate)
#        "SL.UEM.TOTL.NE.ZS",   # Unemployment, total (% of total labor force) (national estimate)
        "GC.BAL.CASH.GD.ZS",   # Cash surplus/deficit (% of GDP)
        "FP.CPI.TOTL.ZG"       # Inflation, consumer prices (annual %) 
    ]
]

years = [2007, 2008, 2009, 2010, 2011, 2012]
eu = indicators_t[indicators_t.year.isin(years)]\
#                 [indicators_t.country.isin(euCodes)]\
                 .select(["country", "year"] + features)

sqlContext.registerDataFrameAsTable(eu, "eu")


```


## 5.2 Create KMeans Pipeline

Note: Input columns (features) need to be in `Vector` format. `pyspark.ml.feature.VectorAssembler` allows to pipeline this


```python
%pyspark

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml import Pipeline

assembler = VectorAssembler(inputCols=features, outputCol="features")

kmeans = KMeans(k=3, seed=42)

pipeline = Pipeline(stages=[assembler, kmeans])
model = pipeline.fit(eu)

transformed = model.transform(eu).select("country", "year", "prediction").sort(["country", "year"])
sqlContext.registerDataFrameAsTable(transformed, "Classes")

```


## 5.3 Visualize Countries in classes over years

**Caveat**: The classes are **not** to interprete as an ordered list, they are complete random!


```sql
%sql

select country, year,  prediction + 1 as class
from Classes 
order by country, year

```


## 5.4 Show countries per year and class as lists

Note: There is no function for `GroupedData` to collect values as list. Hence, back to `RDD` and `aggregateByKey`


```python
%pyspark

def seq(u, v):
    if u == None: u = []
    u.append(v.country)
    return u

def comb(u1, u2):
    return u1 + u2

data = transformed.select(["year", "country", "prediction"])\
                  .rdd\
                  .keyBy(lambda row: str(row.year) + ":" + str(row.prediction))\
                  .aggregateByKey(None, seq, comb)\
                  .sortByKey()\
                  .map(lambda tuple: (tuple[0], ", ".join(sorted(tuple[1]))))

year = ""
for c in data.collect():
    y, cl = c[0].split(":")
    if y != year:
        print "\nYear:", y
        year = y
    print cl, "=", c[1]


```


### Little helper for indicator codes


```sql
%sql

select distinct IndicatorCode, IndicatorName from IndicatorsRDD
where indicatorName like "%nflat%"
order by IndicatorCode
```

