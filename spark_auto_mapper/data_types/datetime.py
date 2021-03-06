from typing import Optional, List

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import coalesce, to_timestamp

from spark_auto_mapper.data_types.column import AutoMapperDataTypeColumn
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.defined_types import AutoMapperDateInputType


class AutoMapperDateTimeDataType(AutoMapperDataTypeBase):
    def __init__(
        self,
        value: AutoMapperDateInputType,
        formats: Optional[List[str]] = None
    ) -> None:
        """
        Converts the value to a timestamp type in Spark


        :param value: value
        :param formats: (Optional) formats to use for trying to parse the value otherwise uses Spark defaults
        """
        super().__init__()

        self.value: AutoMapperDataTypeBase = value \
            if isinstance(value, AutoMapperDataTypeBase) \
            else AutoMapperValueParser.parse_value(value)
        self.formats: Optional[List[str]] = formats

    def get_column_spec(
        self, source_df: Optional[DataFrame], current_column: Optional[Column]
    ) -> Column:
        # if column is not of type date then convert it to date
        formats_column_specs: List[Column] = [
            to_timestamp(
                self.value.get_column_spec(
                    source_df=source_df, current_column=current_column
                ),
                format=format_
            ) for format_ in self.formats
        ] if self.formats else [
            to_timestamp(
                self.value.get_column_spec(
                    source_df=source_df, current_column=current_column
                )
            )
        ]
        if source_df is not None and isinstance(self.value, AutoMapperDataTypeColumn) \
                and not dict(source_df.dtypes)[self.value.value] == "timestamp":
            return coalesce(
                *formats_column_specs
            ) if len(formats_column_specs) > 1 else formats_column_specs[0]
        elif isinstance(self.value, AutoMapperDataTypeLiteral):
            return coalesce(
                *formats_column_specs
            ) if len(formats_column_specs) > 1 else formats_column_specs[0]
        else:
            column_spec = self.value.get_column_spec(
                source_df=source_df, current_column=current_column
            )
            return column_spec
