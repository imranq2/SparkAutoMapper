from typing import Dict, Optional

from pyspark.sql import SparkSession, Column, DataFrame
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.data_types.complex.complex_base import AutoMapperDataTypeComplexBase
from spark_auto_mapper.data_types.list import AutoMapperList
from spark_auto_mapper.data_types.number import AutoMapperNumberDataType
from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
from spark_auto_mapper.type_definitions.defined_types import AutoMapperDateInputType


class MyProcessingStatusExtensionItem(AutoMapperDataTypeComplexBase):
    # noinspection PyPep8Naming
    def __init__(
        self,
        url: str,
        valueString: Optional[AutoMapperTextLikeBase] = None,
        valueDateTime: Optional[AutoMapperDateInputType] = None
    ) -> None:
        super().__init__(
            url=url, valueString=valueString, valueDateTime=valueDateTime
        )


class MyProcessingStatusExtension(AutoMapperDataTypeComplexBase):
    # noinspection PyPep8Naming
    def __init__(
        self,
        processing_status: AutoMapperTextLikeBase,
        request_id: AutoMapperTextLikeBase,
        date_processed: Optional[AutoMapperDateInputType] = None,
    ) -> None:
        definition_base_url = "https://raw.githubusercontent.com/imranq2/SparkAutoMapper.FHIR/main/StructureDefinition/"
        processing_status_extensions = [
            MyProcessingStatusExtensionItem(
                url="processing_status",
                valueString=processing_status,
            ),
            MyProcessingStatusExtensionItem(
                url="request_id",
                valueString=request_id,
            ),
        ]
        if date_processed:
            processing_status_extensions.append(
                MyProcessingStatusExtensionItem(
                    url="date_processed",
                    valueDateTime=date_processed,
                )
            )
        self.extensions = processing_status_extensions
        super().__init__(
            url=definition_base_url,
            extension=AutoMapperList(processing_status_extensions)
        )

    def include_null_properties(self, include_null_properties: bool) -> None:
        for item in self.extensions:
            item.include_null_properties(
                include_null_properties=include_null_properties
            )


class MyClass(AutoMapperDataTypeComplexBase):
    def __init__(
        self, name: AutoMapperTextLikeBase, age: AutoMapperNumberDataType,
        extension: AutoMapperList[MyProcessingStatusExtension]
    ) -> None:
        super().__init__(name=name, age=age, extension=extension)

    def get_schema(self, include_extension: bool) -> Optional[StructType]:
        schema: StructType = StructType(
            [
                StructField("name", StringType(), False),
                StructField("age", IntegerType(), True),
            ]
        )
        return schema


def test_auto_mapper_complex_with_extension(
    spark_session: SparkSession
) -> None:
    # Arrange
    spark_session.createDataFrame(
        [
            (1, 'Qureshi', 'Imran', 45),
            (2, 'Vidal', 'Michael', 35),
        ], ['member_id', 'last_name', 'first_name', 'my_age']
    ).createOrReplaceTempView("patients")

    source_df: DataFrame = spark_session.table("patients")

    df = source_df.select("member_id")
    df.createOrReplaceTempView("members")

    # Act
    mapper = AutoMapper(
        view="members",
        source_view="patients",
        keys=["member_id"],
        drop_key_columns=False
    ).complex(
        MyClass(
            name=A.column("last_name"),
            age=A.number(A.column("my_age")),
            extension=AutoMapperList(
                [
                    MyProcessingStatusExtension(
                        processing_status=A.text("foo"),
                        request_id=A.text("bar"),
                        date_processed=A.datetime("2019-07-01 12:01:19.000")
                    )
                ]
            )
        )
    )

    assert isinstance(mapper, AutoMapper)
    sql_expressions: Dict[str, Column] = mapper.get_column_specs(
        source_df=source_df
    )
    for column_name, sql_expression in sql_expressions.items():
        print(f"{column_name}: {sql_expression}")

    result_df: DataFrame = mapper.transform(df=df)

    # Assert
    assert str(sql_expressions["name"]
               ) == str(col("b.last_name").cast("string").alias("name"))
    assert str(sql_expressions["age"]
               ) == str(col("b.my_age").cast("int").alias("age"))

    result_df.printSchema()
    result_df.show(truncate=False)

    assert result_df.where("member_id == 1"
                           ).select("name").collect()[0][0] == "Qureshi"

    assert dict(result_df.dtypes)["age"] == "int"
