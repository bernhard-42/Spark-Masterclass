## Data

1) `iris.data`

- Source: [https://archive.ics.uci.edu/ml/machine-learning-databases/iris/](https://archive.ics.uci.edu/ml/machine-learning-databases/iris/)
- Changes: None
- Preparation

		hdfs dfs -put iris.data /tmp

2) `europe-indicators.csv.gz`

- Source: [https://www.kaggle.com/worldbank/.../world-development-indicators-release-2016-01-28-06-31-53.zip](https://www.kaggle.com/worldbank/world-development-indicators/downloads/world-development-indicators-release-2016-01-28-06-31-53.zip)
- See also: [https://www.kaggle.com/worldbank/world-development-indicators](https://www.kaggle.com/worldbank/world-development-indicators)
- Changes: Reduced to a subset with countries of the European Union only  (AUT, BEL, BUL, CYP, CZE, DEU, DNK, ESP, EST, FIN, FRA, GBR, GRC, HRV, HUN, IRL, ITA, LTU, LUX, LVA, MLT, NLD, POL, PRT, ROM, SVK, SVN, SWE) 
- Preparation

		gzip -d europe-indicators.csv.gz
		hdfs dfs -put europe-indicators.csv /tmp


## Add Notebook to Zeppelin

- Preparation

		cp -R "2BB5HRGZ2" "/opt/in cubator-zeppelin/notebook/"
		chown -R zeppelin:zeppelin "/opt/in cubator-zeppelin/notebook/2BB5HRGZ2"

	and restart Apache Zeppelin


## Code View without Zepopelin

Click on [Code.md](./Code.md)