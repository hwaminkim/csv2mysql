# How To Use:

1. Set config file
    Copy example config file
    ```
    $ cp config.yaml.example config.yaml
    $ vim config.yaml
    ```

    set `config.yaml`
    ```
    mysql:
        host: "localhost"
        port: 3306
        user: "<ID>"
        password: "<PASSWORD>"
        database: "<DB_NAME>"
    ```

2. Prepare .csv file
	Example csv file `example.csv` (Check `csv/example.csv.example` file)
	```
	hom_id,hom_source_id,hom_source_name,ode_gene_id,sp_id,hom_date
	11542,11542,Homologene,199311,4,2020-05-04
	11542,11542,Homologene,509899,10,2020-05-04
	11542,11542,Homologene,547318,11,2020-05-04
	```

3. Run script
	```
	$ pipenv run python csv2mysql.py -t <TABLE_NAME> -f <CSV_FILE_NAME>
	```
