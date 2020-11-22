from typing import TypeVar, Union, Generic, Optional, cast, List

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import when

from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.wrapper_types import AutoMapperColumnOrColumnLikeType, AutoMapperAnyDataType

_TAutoMapperDataType = TypeVar(
    "_TAutoMapperDataType", bound=AutoMapperAnyDataType
)


class AutoMapperIfDataType(
    AutoMapperDataTypeBase, Generic[_TAutoMapperDataType]
):
    """
    If check returns value if the checks passes else when_not
    """
    def __init__(
        self,
        column: AutoMapperColumnOrColumnLikeType,
        check: Union[AutoMapperAnyDataType, List[AutoMapperAnyDataType]],
        value: _TAutoMapperDataType,
        else_: Optional[_TAutoMapperDataType] = None
    ):
        super().__init__()

        self.column: AutoMapperColumnOrColumnLikeType = column
        if isinstance(check, list):
            self.check: Union[AutoMapperDataTypeBase,
                              List[AutoMapperDataTypeBase]] = [
                                  a if isinstance(a, AutoMapperDataTypeBase)
                                  else AutoMapperValueParser.parse_value(a)
                                  for a in check
                              ]
        else:
            self.check = check \
                if isinstance(check, AutoMapperDataTypeBase) \
                else AutoMapperValueParser.parse_value(check)
        self.value: AutoMapperDataTypeBase = value \
            if isinstance(value, AutoMapperDataTypeBase) \
            else AutoMapperValueParser.parse_value(value)
        if else_:
            self.else_: AutoMapperDataTypeBase = cast(AutoMapperDataTypeBase, else_) \
                if isinstance(value, AutoMapperDataTypeBase) \
                else AutoMapperValueParser.parse_value(value)
        else:
            self.else_ = AutoMapperDataTypeLiteral(None)

    def include_null_properties(self, include_null_properties: bool) -> None:
        self.value.include_null_properties(
            include_null_properties=include_null_properties
        )

    def get_column_spec(self, source_df: DataFrame) -> Column:
        if isinstance(self.check, list):
            column_spec = when(
                self.column.get_column_spec(source_df=source_df).isin(
                    *[
                        c.get_column_spec(source_df=source_df)
                        for c in self.check
                    ]
                ), self.value.get_column_spec(source_df=source_df)
            ).otherwise(self.else_.get_column_spec(source_df=source_df))
        else:
            column_spec = when(
                self.column.get_column_spec(source_df=source_df).eqNullSafe(
                    self.check.get_column_spec(source_df=source_df)
                ), self.value.get_column_spec(source_df=source_df)
            ).otherwise(self.else_.get_column_spec(source_df=source_df))

        return column_spec