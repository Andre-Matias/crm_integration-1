### Liquidity's probability model ###

Technically we have 4 different models:
* Imovirtual, apartments to sell
* Imovirtual, apartments to rent
* Otodom, apartments to sell
* Otodom, apartments to rent

Training process described in the notebooks. 
Every notebook has a prerequisites section, where you can find input files list.
This files can be obtained with sql queries (models/sql_queries).

1.* - Parsing `params` attributes
2.* - Data preprocessing and datasetes building
3.* - Model training, usage examples.

If you want to use retrained models in the service - put the output of 3.* to the `/service/data`.

### Libraries list

* `Flask==0.12.1`
* `numpy==1.11.2`
* `pandas==0.19.0`
* `eli5==0.8`
* `scikit-learn==0.18.1`
* `xgboost==0.6a2`

