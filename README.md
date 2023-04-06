# Projet_Archi_distribuees
Développement d'une pipeline big data


Membres :
•	BENMECHICHE Khaled 
•	AMEZIANE Liticia 
•	MESBAHI Lydia

Le but de notre projet est de voir à travers différents traitements et visualisations s’il existe une relation entre les disponibilités des vélib (Usage des vélos) et les facteurs météorologiques. 

Nous avons utilisé deux principales sources de données : 

API 1 : Vélib - Vélos et bornes - Disponibilité temps réel 
Source : Vélib - Vélos et bornes - Disponibilité temps réel — Paris Data

On a choisi l’API Vélib Métropole car c’est le plus grand service de vélos partagés au monde incluant des vélos électriques rechargeables en station. Il permet de connaître en temps réel le nombre de vélos mécaniques/électriques à chaque station ainsi que le nombre de bornettes libres. 

L’API nous fournit : 
-	stationcode : numéro unique d’identification de la station. Ce numéro identifie la station au sein du service Vélib’ Métropole 
-	name : nom de la station 
-	is_installed : variable binaire indiquant si la station a déjà été déployée ou est encore en cours de déploiement 
-	is_renting : variable binaire indiquant si la station peut louer des vélos 
-	is_returning : variable binaire indiquant si la station peut recevoir des vélos 
-	duedate : date de la dernière mise-à-jour 
-	numbikesavailable : nombre de vélos disponibles 
-	numdocksavailable : nombre de bornettes disponibles 
-	capacity : nombre de vélos que la station peut recevoir 
-	mechanical : nombre de vélos mécanique disponible 
-	ebike : nombre de vélos électriques disponible 

Les données sont actualisées chaque heure. 

Les paramètres de l’API : 
-	Rows : le nombre de ligne qu’on veut récupérer (nous avons choisi 500 
lignes) 
-	Sort : Critère de tri (nous avons trié par date d’actualisation des données ‘duedate’) 
-	Facet : Nom des facettes à activer dans les résultats 
●	nom_arrondissement_communes 
●	is_installed 
-	Refine : Les conditions à appliquer afin de faire une sélection précise. Dans notre cas on veut cibler les données sur Paris où la station a déjà été déployée : 
●	nom_arrondissement_communes = Paris 
●	is_installed = OUI 
-	Timezone : Le fuseau horaire utilisé pour interpréter les dates et heures dans la requête et les données de la réponse. Le fuseau horaire pris en considération. 
●	Europe/Paris 

La réponse et les clés possibles : 

-	Réponse : 
https://opendata.paris.fr/api/records/1.0/search/?dataset=velibdisponibilite-en-tempsreel&q=&rows=500&sort=duedate&facet=is_installed&facet=nom_arrondis sement_communes&refine.nom_arrondissement_communes=Paris&refine
.is_installed=OUI&timezone=Europe%2FParis 
-	Clés possibles :  sans clé. 
 
  
API 2 : Météo 

Source : Météo sur votre site Web avec JSON - Tutiempo 
C’est une API qui fournit des informations sur des données météorologiques selon la localité et la ville. 
L’API nous fournit : 
-	Température (La température) 
-	wind (La vitesse du vent) 
-	humidity (L’humidité) 
-	ciel (Le temps) 
Les données sont actualisées chaque heure 
Les paramètres de l’API : 
https://api.tutiempo.net/json/?lan={lan}&apid={Clé API}&lid={lid} 
-	Langue de sortie : {lan} 
●	es - espagnol 
●	en - anglais 
●	fr - français 
●	pt - portugais 
●	de - allemand 
●	it - italien 
-	Clé de l’API : {Clé API} 
-	ID de la ville : {lid} 
Pour avoir l’ID de la ville, il faut sélectionner un pays ensuite une division et enfin une ville. 
Nous avons choisi France - Ile-de-France - Paris et nous avons obtenu comme ID : 39720 
-	lan : “fr” 
-	clé API : “zsTzaXzqqqXbxsh” 
-	lid : “39720” 

Réponse : https://api.tutiempo.net/json/?lan=fr&apid=zsTzaXzqqqXbxsh&lid=39720 


 --Les technologies utilisées :
•	SparkStreaming
•	Kafka 
•	Docker 
•	MongodbAtlas
•	PowerBI 
•	VisualStudioCode
•	TRELLO
•	JupyterNotebook 

  
  
 
