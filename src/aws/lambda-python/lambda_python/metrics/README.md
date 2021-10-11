**Note:** this package (`metrics`) contains code which is required for model serving on Lambda function side. It'll be moved to a separate package in the near future.

**Running**

Run `ranking_metrics.py` script with the right database uri to evaluate MAP for the following two rankings:
* Baseline: `TFCollectionRanking`
* Enhanced: `TFIDFWithNorm1_5CollectionRanking`

Command:
```bash
ranking_metrics.py -d postgresql://postgres:postgrespassword@localhost:5432/postgres`
```