# BDT
CVUT FEL BDT course

## Useful links

* [Metacentrum hadoop reference page](https://wiki.metacentrum.cz/wiki/Hadoop)
* [HDFS DFS commands](https://hadoop.apache.org/docs/r2.7.5/hadoop-project-dist/hadoop-common/FileSystemShell.html)
* [Learn python in Y minutes](https://learnxinyminutes.com/docs/python3/)
* [Official python documentation](https://docs.python.org/3/)
* [Regular expressions at Ryan's tutorials](https://ryanstutorials.net/regular-expressions-tutorial/regular-expressions-basics.php)
* [Hive language manual](https://cwiki.apache.org/confluence/display/Hive/LanguageManual)
* [Apache Spark manual](https://spark.apache.org/docs/1.6.0/)
* [PySpark SQL manual](http://spark.apache.org/docs/1.6.0/api/python/pyspark.sql.html)
* [CSV files import/export](https://github.com/databricks/spark-csv)


## Connect to hadoop metacentrum server
https://dashboard.cloud.muni.cz/project/instances/
1. Use private key to connect to the instance (second IP address) 
`sudo ssh -i /Users/lindepav/.ssh/bdt_kp4.pem debian@147.251.115.238`
2. (initial setup)
- Generated password: h2rovW/6
`sudo /usr/local/sbin/hadoop-single-setup.sh`
3. Set Kerberos HTTP SPNEGO authentication by runnning `kinit`
- enter password from `password.txt`
4. This is linux (debian type) edge node, to go to hadoop, use `hdfs` commands 


### HDFS commands (more in `hdfs-cheatsheet.pdf`)
`hdfs dfs -akce [param] [soubor/adresář]`
- `ls` → výpis adresáře
- `mkdir` → vytvoření adresáře
- `cp` → kopírování v rámci HDFS
- `mv` → přesun v rámci HDFS
- `rm` → mazání souboru nebo adresáře
- `put` → kopírování z lokálního FS na HDFS
- `get` → kopírování z HDFS na lokální FS
- `cat` → výpis obsahu souboru
- `chmod` → změna přístupových práv

### Foldering on server/hadoop
#### Linux (edge node)
- umístění: `/home/username` (nebo podobně)
- lze se přesunout příkazem `cd` → relativní cesta vede
z aktuální polohy (zjistíme příkazem `pwd`)
#### HDFS
- umístění: `/user/username`
- **nelze se přesunout** → relativní cesta vede vždy
odtud

