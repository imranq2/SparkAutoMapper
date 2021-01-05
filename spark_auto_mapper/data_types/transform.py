from typing import Generic, Optional, TypeVar, Union

from pyspark.sql import DataFrame, Column

from spark_auto_mapper.data_types.array_base import AutoMapperArrayBase
from spark_auto_mapper.helpers.spark_higher_order_functions import transform
from spark_auto_mapper.type_definitions.wrapper_types import AutoMapperAnyDataType, AutoMapperColumnOrColumnLikeType

_TAutoMapperDataType = TypeVar(
    "_TAutoMapperDataType", bound=AutoMapperAnyDataType
)


class AutoMapperTransformDataType(
    AutoMapperArrayBase, Generic[_TAutoMapperDataType]
):
    def __init__(
        self, column: Union[AutoMapperArrayBase,
                            AutoMapperColumnOrColumnLikeType],
        value: _TAutoMapperDataType
    ) -> None:
        super().__init__()

        self.column: Union[AutoMapperArrayBase,
                           AutoMapperColumnOrColumnLikeType] = column
        self.value: _TAutoMapperDataType = value

    def get_column_spec(
        self, source_df: DataFrame, current_column: Optional[Column]
    ) -> Column:
        column_spec: Column = self.column.get_column_spec(
            source_df=source_df, current_column=current_column
        )

        def get_column_spec_for_column(x: Column) -> Column:
            value_get_column_spec: Column = self.value.get_column_spec(
                source_df=source_df, current_column=x
            )
            return value_get_column_spec

        return transform(column_spec, get_column_spec_for_column)