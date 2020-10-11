from pyspark.sql import SparkSession, Column, DataFrame
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import lit

from spark_auto_mapper.automapper import AutoMapper


def test_auto_mapper_with_column_literal(spark_session: SparkSession):
    # Arrange
    spark_session.createDataFrame(
        [
            (1, 'Qureshi', 'Imran'),
            (2, 'Vidal', 'Michael'),
        ],
        ['member_id', 'last_name', 'first_name']
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members",
        source_view="patients",
        keys=["member_id"]
    ).withColumn(
        dst_column="lname",
        value="last_name"
    )

    sql_expression: Column = mapper.get_column_spec()
    print(sql_expression)

    assert str(sql_expression) == str(lit("last_name").alias("lname"))

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    result_df.printSchema()
    result_df.show()

    assert result_df.where("member_id == 1").select("lname").collect()[0][0] == "last_name"
    assert result_df.where("member_id == 2").select("lname").collect()[0][0] == "last_name"
