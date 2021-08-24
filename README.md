# final_project_rd

в airflow.connections нужно добавить:

Conn Id: dshop

Host: localhost

Login: pguser

Schema: dshop

Password: secret

--------------------------------------

Conn Id: dshop_bu

Host: localhost

Login: pguser

Schema: dshop_bu

Password: secret

--------------------------------------
Conn Id: greenplum

Host: localhost

Login: pguser

Schema: public

Password: secret

--------------------------------------
В airflow.variables

api	{ "url": "https://robot-dreams-de-api.herokuapp.com", "endpoint": "/out_of_stock", "auth": "JWT" }

auth	{"endpoint": "/auth", "payload": {"username": "rd_dreams", "password": "djT6LasE"}}

tables	{ "dshop": ["aisles", "clients", "departments", "orders", "products"], "dshop_bu": ["aisles", "clients", "departments", "orders", "products", "stores", "store_types", "location_areas"] }	
 


