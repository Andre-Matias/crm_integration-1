# README #

Here you can find a service for prediction listing probability to be liquid.
This service works for Otodom.pl and Imovirtual.pt, apartments for sell and
and apartments for rent. 

### How do I get set up? ###

You need 

* Docker server to run container
* Docker composer to build images/start containers
* Unzip stored models with `unzip data.zip`

### Docker ###

build new version: `sh docker-build.sh`

start generic docker container: `sh docker-up.sh `


### How do I can use it? ###

By default, the app will be available at: http://127.0.0.1:5000

#### Sample call, Otodom rent ####

```json
curl http://127.0.0.1:5000/predict -X POST -H "Content-Type: application/json" -d '
   {"market": "pl",
	"section": "rent", 
	"properties": [{"city_id": 26, 
                    "mysql_search_rooms_num": 3, 
					"mysql_search_m": 88, 
					"mysql_search_price": 2000,
					"mysql_search_price_per_m": 50,
					"title": 30,
        			"description": 1786,
        			"rent_to_students": 1,
    				"private_business": "business",
    				"street_name" : "Vodickova",
    				"build_year" : 2000,
    				"building_floors_num": 5, 
    				"building_material": "brick",
    				"construction_status": "ready_to_use",
    				"extras_types": "balcony<->garage<->lift<->separate_kitchen",
    				"heating": "urban",
    				"media_types": "cable-television<->internet",
    				"security_types": "monitoring",
    				"windows_type": "wooden"
    }]}'
```

#### Sample call Otodom sell ####

```json
{"market": "pl",
	"section": "sell", 
	"explain": 1,
	"properties": [{"city_id": 180, 
					"mysql_search_rooms_num": 2, 
					"mysql_search_m": 51.0, 
					"mysql_search_price": 99000,
        			"title": 47,
        			"description": 978,
        			"building_floors_num": 2, 
        			"building_material": "brick",
        			"equipment_types": "balcony<->separate_kitchen",
        			"windows_types": "plastic",
    				"private_business": "private",
    				"market": "secondary",
    				"street_name" : "Vodickova"
	}]}
```

#### Sample call Imovirtual  ####

```json
curl http://127.0.0.1:5000/predict -X POST -H "Content-Type: application/json" -d '
{"market": "pt",
	"section": "rent", 
	"explain": 1,
	"properties": [{"city_id": 11549713, 
					"rooms_num": 4, 
					"mysql_search_m": 80, 
					"mysql_search_price": 1000,
					"title": 30,
        			"description": 1000,
    				"private_business": "business",
    				"street_name" : "Vodickova",
    				"n_images2": 22,
    				"construction_year": 1996
	}]}'
```



### How do I can reproduce/retrain models? ###

You can find jupyter notebooks in the models directory.

### Authors ###

Anastasiia Kornilova `anastasiia.kornilova@rebbix.com`