# README #

This is a python script that maintain a database with all the free synop data (meteo-france data since 1996, measurment every 3h)
free data = temperature, humidity, and much more updated every 3h since 1996

just run it (type in cmd : python synop_db_maintainer.py) and it will create a sqlite database in '/db/synop.db' and store the synop data, it handles duplicates so if you run it 2 days apart it will just retrieve/insert new data in the DB

### what's inside the DB? ###

Inside the DB you will find one table : 'synop_data'

Not every data is stored, I only took care of those that I was interested in :

 * station id
 * date as a timestamp (France time)
 * sea level pressure
 * 10 minute average wind speed
 * temperature
 * humidity
 * snow height
 * precipitation during the last 24h

### What are those synop data ? ###

Synop data are free meteo data from meteo-france stations given as CSVs.

 * what can be find inside those synop CSVs : https://donneespubliques.meteofrance.fr/client/document/doc_parametres_synop_168.pdf
 * List of all synop stations : https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/postesSynop.csv
 * where to download manually : https://donneespubliques.meteofrance.fr/?fond=produit&id_produit=90&id_rubrique=32

### what to do with those data ###

You can make graphs, run : temperatures_near_paris_1996.py
It will make an html file in {base_dir}/graphs, double clic it, it will open it in your internet browser, this is an interactive graph of temperature in 1996 near paris.

![1996 orly temperatures](/img_for_readme/1996temperatures.png?raw=true "1996 orly temperatures")

### I get an error !! ###

if it look like this :

ImportError: No module named plotly.graph_objs

it means you need to install plotly, open cmd and type 'pip install plotlu', without the '' of course.
