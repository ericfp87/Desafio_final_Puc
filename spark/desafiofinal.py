from pyspark.sql import functions as f


schema = "PassengerId int, Survived int, Pclass int, Name string, Sex string, Age double, SibSp int, Parch int, Ticket string, Fare double, Cabin string, Embarked string"
desafiofinal = (
    spark
    .read
    .csv("s3://desafiofinal.puc/titanic.csv", 
        header=True, schema=schema, sep=";")
)
desafiofinal.printSchema()


(
    desafiofinal
    .write
    .format('parquet')
    .save("s3://desafiofinal.puc/parquet/")

)


desafiofinalparquet = (
    spark
    .read
    .parquet("s3://desafiofinal.puc/parquet/")
)


(
    desafiofinalparquet
    .where("PassengerId < 50")
    .where("Pclass = 3")
    .where("Embarked = 'S' OR Embarked = 'Q'")
    .show()

)