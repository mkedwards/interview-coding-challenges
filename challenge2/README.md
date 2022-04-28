Code challenge 
================

We want to examine which actors are consistently cast in high-budget movies, 
so we want to write a spark job that from the given data , 
finds out the top 10 actors for each gender, rated by average budget. 
We want to limit to actors who've been in at least 5 movies.

_Note_: columns contain nested JSON that needs to be decoded.

What does the result look like ? Is it what you expect ?
If not, change the query to improve the quality of the result.

We want to see the top actors, how to fix it ? 

Data
--------------

Data is in src/main/resources.

(from https://www.kaggle.com/tmdb/tmdb-movie-metadata)


#### Movie Data
```
+---------+--------------------+--------------------+------+--------------------+-----------------+--------------------+--------------------+----------+--------------------+--------------------+------------+----------+-------+--------------------+--------+--------------------+--------------------+------------+----------+
|   budget|              genres|            homepage|    id|            keywords|original_language|      original_title|            overview|popularity|production_companies|production_countries|release_date|   revenue|runtime|    spoken_languages|  status|             tagline|               title|vote_average|vote_count|
+---------+--------------------+--------------------+------+--------------------+-----------------+--------------------+--------------------+----------+--------------------+--------------------+------------+----------+-------+--------------------+--------+--------------------+--------------------+------------+----------+
|237000000|[{"id": 28, "name...|http://www.avatar...| 19995|[{"id": 1463, "na...|               en|              Avatar|In the 22nd centu...|150.437577|[{"name": "Ingeni...|[{"iso_3166_1": "...|  2009-12-10|2787965087|    162|[{"iso_639_1": "e...|Released|Enter the World o...|              Avatar|         7.2|     11800|
|300000000|[{"id": 12, "name...|http://disney.go....|   285|[{"id": 270, "nam...|               en|Pirates of the Ca...|Captain Barbossa,...|139.082615|[{"name": "Walt D...|[{"iso_3166_1": "...|  2007-05-19| 961000000|    169|[{"iso_639_1": "e...|Released|At the end of the...|Pirates of the Ca...|         6.9|      4500|
|245000000|[{"id": 28, "name...|http://www.sonypi...|206647|[{"id": 470, "nam...|               en|             Spectre|A cryptic message...|107.376788|[{"name": "Columb...|[{"iso_3166_1": "...|  2015-10-26| 880674609|    148|[{"iso_639_1": "f...|Released|A Plan No One Esc...|             Spectre|         6.3|      4466|
|250000000|[{"id": 28, "name...|http://www.thedar...| 49026|[{"id": 849, "nam...|               en|The Dark Knight R...|Following the dea...| 112.31295|[{"name": "Legend...|[{"iso_3166_1": "...|  2012-07-16|1084939099|    165|[{"iso_639_1": "e...|Released|     The Legend Ends|The Dark Knight R...|         7.6|      9106|
|260000000|[{"id": 28, "name...|http://movies.dis...| 49529|[{"id": 818, "nam...|               en|         John Carter|John Carter is a ...| 43.926995|[{"name": "Walt D...|[{"iso_3166_1": "...|  2012-03-07| 284139100|    132|[{"iso_639_1": "e...|Released|Lost in our world...|         John Carter|         6.1|      2124|
|258000000|[{"id": 14, "name...|http://www.sonypi...|   559|[{"id": 851, "nam...|               en|        Spider-Man 3|The seemingly inv...|115.699814|[{"name": "Columb...|[{"iso_3166_1": "...|  2007-05-01| 890871626|    139|[{"iso_639_1": "e...|Released|  The battle within.|        Spider-Man 3|         5.9|      3576|
|260000000|[{"id": 16, "name...|http://disney.go....| 38757|[{"id": 1562, "na...|               en|             Tangled|When the kingdom'...| 48.681969|[{"name": "Walt D...|[{"iso_3166_1": "...|  2010-11-24| 591794936|    100|[{"iso_639_1": "e...|Released|They're taking ad...|             Tangled|         7.4|      3330|
|280000000|[{"id": 28, "name...|http://marvel.com...| 99861|[{"id": 8828, "na...|               en|Avengers: Age of ...|When Tony Stark t...|134.279229|[{"name": "Marvel...|[{"iso_3166_1": "...|  2015-04-22|1405403694|    141|[{"iso_639_1": "e...|Released| A New Age Has Come.|Avengers: Age of ...|         7.3|      6767|
|250000000|[{"id": 12, "name...|http://harrypotte...|   767|[{"id": 616, "nam...|               en|Harry Potter and ...|As Harry begins h...| 98.885637|[{"name": "Warner...|[{"iso_3166_1": "...|  2009-07-07| 933959197|    153|[{"iso_639_1": "e...|Released|Dark Secrets Reve...|Harry Potter and ...|         7.4|      5293|
|250000000|[{"id": 28, "name...|http://www.batman...|209112|[{"id": 849, "nam...|               en|Batman v Superman...|Fearing the actio...|155.790452|[{"name": "DC Com...|[{"iso_3166_1": "...|  2016-03-23| 873260194|    151|[{"iso_639_1": "e...|Released|  Justice or revenge|Batman v Superman...|         5.7|      7004|
|270000000|[{"id": 12, "name...|http://www.superm...|  1452|[{"id": 83, "name...|               en|    Superman Returns|Superman returns ...| 57.925623|[{"name": "DC Com...|[{"iso_3166_1": "...|  2006-06-28| 391081192|    154|[{"iso_639_1": "e...|Released|                null|    Superman Returns|         5.4|      1400|
|200000000|[{"id": 12, "name...|http://www.mgm.co...| 10764|[{"id": 627, "nam...|               en|   Quantum of Solace|Quantum of Solace...|107.928811|[{"name": "Eon Pr...|[{"iso_3166_1": "...|  2008-10-30| 586090727|    106|[{"iso_639_1": "e...|Released|For love, for hat...|   Quantum of Solace|         6.1|      2965|
|200000000|[{"id": 12, "name...|http://disney.go....|    58|[{"id": 616, "nam...|               en|Pirates of the Ca...|Captain Jack Spar...|145.847379|[{"name": "Walt D...|[{"iso_3166_1": "...|  2006-06-20|1065659812|    151|[{"iso_639_1": "e...|Released|       Jack is back!|Pirates of the Ca...|         7.0|      5246|
|255000000|[{"id": 28, "name...|http://disney.go....| 57201|[{"id": 1556, "na...|               en|     The Lone Ranger|The Texas Rangers...| 49.046956|[{"name": "Walt D...|[{"iso_3166_1": "...|  2013-07-03|  89289910|    149|[{"iso_639_1": "e...|Released|Never Take Off th...|     The Lone Ranger|         5.9|      2311|
|225000000|[{"id": 28, "name...|http://www.manofs...| 49521|[{"id": 83, "name...|               en|        Man of Steel|A young boy learn...| 99.398009|[{"name": "Legend...|[{"iso_3166_1": "...|  2013-06-12| 662845518|    143|[{"iso_639_1": "e...|Released|You will believe ...|        Man of Steel|         6.5|      6359|
|225000000|[{"id": 12, "name...|                null|  2454|[{"id": 818, "nam...|               en|The Chronicles of...|One year after th...| 53.978602|[{"name": "Walt D...|[{"iso_3166_1": "...|  2008-05-15| 419651413|    150|[{"iso_639_1": "e...|Released|Hope has a new face.|The Chronicles of...|         6.3|      1630|
|220000000|[{"id": 878, "nam...|http://marvel.com...| 24428|[{"id": 242, "nam...|               en|        The Avengers|When an unexpecte...|144.448633|[{"name": "Paramo...|[{"iso_3166_1": "...|  2012-04-25|1519557910|    143|[{"iso_639_1": "e...|Released|Some assembly req...|        The Avengers|         7.4|     11776|
|380000000|[{"id": 12, "name...|http://disney.go....|  1865|[{"id": 658, "nam...|               en|Pirates of the Ca...|Captain Jack Spar...|135.413856|[{"name": "Walt D...|[{"iso_3166_1": "...|  2011-05-14|1045713802|    136|[{"iso_639_1": "e...|Released|Live Forever Or D...|Pirates of the Ca...|         6.4|      4948|
|225000000|[{"id": 28, "name...|http://www.sonypi...| 41154|[{"id": 4379, "na...|               en|      Men in Black 3|Agents J (Will Sm...| 52.035179|[{"name": "Amblin...|[{"iso_3166_1": "...|  2012-05-23| 624026776|    106|[{"iso_639_1": "e...|Released|They are back... ...|      Men in Black 3|         6.2|      4160|
|250000000|[{"id": 28, "name...|http://www.thehob...|122917|[{"id": 417, "nam...|               en|The Hobbit: The B...|Immediately after...|120.965743|[{"name": "WingNu...|[{"iso_3166_1": "...|  2014-12-10| 956019788|    144|[{"iso_639_1": "e...|Released|Witness the defin...|The Hobbit: The B...|         7.1|      4760|
+---------+--------------------+--------------------+------+--------------------+-----------------+--------------------+--------------------+----------+--------------------+--------------------+------------+----------+-------+--------------------+--------+--------------------+--------------------+------------+----------+
```

#### Cast Data
```
+--------+--------------------+--------------------+--------------------+
|movie_id|               title|                cast|                crew|
+--------+--------------------+--------------------+--------------------+
|   19995|              Avatar|[{"cast_id": 242,...|[{"credit_id": "5...|
|     285|Pirates of the Ca...|[{"cast_id": 4, "...|[{"credit_id": "5...|
|  206647|             Spectre|[{"cast_id": 1, "...|[{"credit_id": "5...|
|   49026|The Dark Knight R...|[{"cast_id": 2, "...|[{"credit_id": "5...|
|   49529|         John Carter|[{"cast_id": 5, "...|[{"credit_id": "5...|
|     559|        Spider-Man 3|[{"cast_id": 30, ...|[{"credit_id": "5...|
|   38757|             Tangled|[{"cast_id": 34, ...|[{"credit_id": "5...|
|   99861|Avengers: Age of ...|[{"cast_id": 76, ...|[{"credit_id": "5...|
|     767|Harry Potter and ...|[{"cast_id": 3, "...|[{"credit_id": "5...|
|  209112|Batman v Superman...|[{"cast_id": 18, ...|[{"credit_id": "5...|
|    1452|    Superman Returns|[{"cast_id": 3, "...|[{"credit_id": "5...|
|   10764|   Quantum of Solace|[{"cast_id": 1, "...|[{"credit_id": "5...|
|      58|Pirates of the Ca...|[{"cast_id": 37, ...|[{"credit_id": "5...|
|   57201|     The Lone Ranger|[{"cast_id": 4, "...|[{"credit_id": "5...|
|   49521|        Man of Steel|[{"cast_id": 2, "...|[{"credit_id": "5...|
|    2454|The Chronicles of...|[{"cast_id": 1, "...|[{"credit_id": "5...|
|   24428|        The Avengers|[{"cast_id": 46, ...|[{"credit_id": "5...|
|    1865|Pirates of the Ca...|[{"cast_id": 15, ...|[{"credit_id": "5...|
|   41154|      Men in Black 3|[{"cast_id": 4, "...|[{"credit_id": "5...|
|  122917|The Hobbit: The B...|[{"cast_id": 10, ...|[{"credit_id": "5...|
+--------+--------------------+--------------------+--------------------+
```