from typing import Any, Dict, Union, TypeVar, cast, Optional, List, Callable

from pyspark.sql.types import StringType

from spark_auto_mapper.data_types.array_base import AutoMapperArrayLikeBase
from spark_auto_mapper.data_types.coalesce import AutoMapperCoalesceDataType
from spark_auto_mapper.data_types.datetime import AutoMapperDateTimeDataType
from spark_auto_mapper.data_types.field import AutoMapperDataTypeField
from spark_auto_mapper.data_types.filter import AutoMapperFilterDataType
from spark_auto_mapper.data_types.hash import AutoMapperHashDataType
from spark_auto_mapper.data_types.if_ import AutoMapperIfDataType
from spark_auto_mapper.data_types.if_not_null_or_empty import AutoMapperIfNotNullOrEmptyDataType
from spark_auto_mapper.data_types.if_regex import AutoMapperIfRegExDataType
from spark_auto_mapper.data_types.join_using_delimiter import AutoMapperJoinUsingDelimiterDataType
from spark_auto_mapper.data_types.split_by_delimiter import AutoMapperSplitByDelimiterDataType
from spark_auto_mapper.data_types.text_like_base import AutoMapperTextLikeBase

from spark_auto_mapper.data_types.amount import AutoMapperAmountDataType
from spark_auto_mapper.data_types.boolean import AutoMapperBooleanDataType
from spark_auto_mapper.data_types.column import AutoMapperDataTypeColumn
from spark_auto_mapper.data_types.complex.complex import AutoMapperDataTypeComplex
from spark_auto_mapper.data_types.concat import AutoMapperConcatDataType
from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.date import AutoMapperDateDataType
from spark_auto_mapper.data_types.expression import AutoMapperDataTypeExpression
from spark_auto_mapper.data_types.if_not_null import AutoMapperIfNotNullDataType
from spark_auto_mapper.data_types.literal import AutoMapperDataTypeLiteral
from spark_auto_mapper.data_types.map import AutoMapperMapDataType
from spark_auto_mapper.data_types.number import AutoMapperNumberDataType
from spark_auto_mapper.data_types.complex.struct_type import AutoMapperDataTypeStruct
from spark_auto_mapper.data_types.regex_replace import AutoMapperRegExReplaceDataType
from spark_auto_mapper.data_types.substring import AutoMapperSubstringDataType
from spark_auto_mapper.data_types.substring_by_delimiter import AutoMapperSubstringByDelimiterDataType
from spark_auto_mapper.data_types.transform import AutoMapperTransformDataType
from spark_auto_mapper.data_types.trim import AutoMapperTrimDataType
from spark_auto_mapper.type_definitions.defined_types import AutoMapperAnyDataType, AutoMapperBooleanInputType, \
    AutoMapperAmountInputType, AutoMapperNumberInputType, AutoMapperDateInputType, AutoMapperTextInputType
from spark_auto_mapper.type_definitions.native_types import AutoMapperNativeTextType, AutoMapperNativeSimpleType
from spark_auto_mapper.type_definitions.wrapper_types import AutoMapperWrapperType, AutoMapperColumnOrColumnLikeType

_TAutoMapperDataType = TypeVar(
    "_TAutoMapperDataType",
    bound=Union[AutoMapperNativeSimpleType, AutoMapperDataTypeBase]
)


class AutoMapperHelpers:
    @staticmethod
    def struct(value: Dict[str, Any]) -> AutoMapperDataTypeStruct:
        """
        Creates a struct
        :param value: A dictionary to be converted to a struct
        :return: A struct automapper type
        """
        return AutoMapperDataTypeStruct(value=value)

    @staticmethod
    def complex(**kwargs: AutoMapperAnyDataType) -> AutoMapperDataTypeComplex:
        """
        Creates a complex type.
        :param kwargs: parameters to be used to create the complex type
        :return: A complex automapper type
        """
        return AutoMapperDataTypeComplex(**kwargs)

    @staticmethod
    def column(value: str) -> AutoMapperTextLikeBase:
        """
        Specifies that the value parameter should be used as a column name
        :param value: name of column
        :return: A column automapper type
        """
        return AutoMapperDataTypeColumn(value)

    @staticmethod
    def text(
        value: Union[AutoMapperNativeSimpleType, AutoMapperTextInputType]
    ) -> AutoMapperTextLikeBase:
        """
        Specifies that the value parameter should be used as a literal text
        :param value: text value
        :return: a text automapper type
        """
        return AutoMapperDataTypeLiteral(value, StringType())

    @staticmethod
    def expression(value: str) -> AutoMapperArrayLikeBase:
        """
        Specifies that the value parameter should be executed as a sql expression in Spark
        :param value: sql
        :return: an expression automapper type
        """
        return AutoMapperDataTypeExpression(value)

    @staticmethod
    def date(
        value: AutoMapperDateInputType,
        formats: Optional[List[str]] = None
    ) -> AutoMapperDateDataType:
        """
        Converts a value to date only
        For datetime use the datetime mapper type


        :param value: value
        :param formats: (Optional) formats to use for trying to parse the value otherwise uses:
                        y-M-d
                        yyyyMMdd
                        M/d/y
        """
        return AutoMapperDateDataType(value, formats)

    @staticmethod
    def datetime(
        value: AutoMapperDateInputType,
        formats: Optional[List[str]] = None
    ) -> AutoMapperDateTimeDataType:
        """
        Converts the value to a timestamp type in Spark


        :param value: value
        :param formats: (Optional) formats to use for trying to parse the value otherwise uses Spark defaults
        """
        return AutoMapperDateTimeDataType(value, formats)

    @staticmethod
    def amount(value: AutoMapperAmountInputType) -> AutoMapperAmountDataType:
        """
        Specifies the value should be used as an amount
        :param value:
        :return: an amount automapper type
        """
        return AutoMapperAmountDataType(value)

    @staticmethod
    def boolean(
        value: AutoMapperBooleanInputType
    ) -> AutoMapperBooleanDataType:
        """
        Specifies the value should be used as a boolean
        :param value:
        :return: a boolean automapper type
        """
        return AutoMapperBooleanDataType(value)

    @staticmethod
    def number(value: AutoMapperNumberInputType) -> AutoMapperNumberDataType:
        """
        Specifies value should be used as a number
        :param value:
        :return: a number automapper type
        """
        return AutoMapperNumberDataType(value)

    @staticmethod
    def concat(
        *args: Union[AutoMapperNativeTextType, AutoMapperWrapperType,
                     AutoMapperTextLikeBase, AutoMapperDataTypeBase]
    ) -> AutoMapperConcatDataType:
        """
        concatenates a list of values.  Each value can be a string or a column
        :param args: string or column
        :return: a concat automapper type
        """
        return AutoMapperConcatDataType(*args)

    @staticmethod
    def if_(
        column: AutoMapperColumnOrColumnLikeType,
        check: Union[AutoMapperAnyDataType, List[AutoMapperAnyDataType]],
        value: _TAutoMapperDataType,
        else_: Optional[_TAutoMapperDataType] = None
    ) -> _TAutoMapperDataType:
        """
        Checks if column matches check_value.  Returns value if it matches else else_


        :param column: column to check
        :param check: value to compare the column to
        :param value: what to return if the value matches
        :param else_: what value to assign if check fails
        :return: an if automapper type
        """

        # cast it to the inner type so type checking is happy
        return cast(
            _TAutoMapperDataType,
            AutoMapperIfDataType(
                column=column, check=check, value=value, else_=else_
            )
        )

    @staticmethod
    def if_not_null(
        check: AutoMapperColumnOrColumnLikeType,
        value: _TAutoMapperDataType,
        when_null: Optional[_TAutoMapperDataType] = None
    ) -> _TAutoMapperDataType:
        """
        Checks if `check` is null


        :param check: column to check for null
        :param value: what to return if the value is not null
        :param when_null: what value to assign if check is not
        :return: an if_not_null automapper type
        """

        # cast it to the inner type so type checking is happy
        return cast(
            _TAutoMapperDataType,
            AutoMapperIfNotNullDataType(
                check=check, value=value, when_null=when_null
            )
        )

    @staticmethod
    def if_not_null_or_empty(
        check: AutoMapperColumnOrColumnLikeType,
        value: _TAutoMapperDataType,
        when_null_or_empty: Optional[_TAutoMapperDataType] = None
    ) -> _TAutoMapperDataType:
        """
        Checks if `check` is null or empty.


        :param check: column to check for null
        :param value: what to return if the value is not null
        :param when_null_or_empty: what value to assign if check is not
        :return: an if_not_null automapper type
        """

        # cast it to the inner type so type checking is happy
        return cast(
            _TAutoMapperDataType,
            AutoMapperIfNotNullOrEmptyDataType(
                check=check,
                value=value,
                when_null_or_empty=when_null_or_empty
            )
        )

    @staticmethod
    def map(
        column: AutoMapperColumnOrColumnLikeType,
        mapping: Dict[Optional[AutoMapperTextInputType],
                      AutoMapperAnyDataType],
        default: Optional[AutoMapperAnyDataType] = None
    ) -> AutoMapperDataTypeExpression:
        """
        maps the contents of a column to values
        :param column: column
        :param mapping: A dictionary mapping the contents of the column to other values
                        e.g., {"Y":"Yes", "N": "No"}
        :param default: the value to assign if no value matches
        :return: a map automapper type
        """
        return AutoMapperMapDataType(
            column=column, mapping=mapping, default=default
        )

    @staticmethod
    def left(
        column: AutoMapperColumnOrColumnLikeType, length: int
    ) -> AutoMapperSubstringDataType:
        """
        Take the specified number of first characters in a string

        :param column: column whose contents to use
        :param length: number of characters to take from left
        :return: a concat automapper type
        """
        return AutoMapperSubstringDataType(
            column=column, start=0, length=length
        )

    @staticmethod
    def right(
        column: AutoMapperColumnOrColumnLikeType, length: int
    ) -> AutoMapperSubstringDataType:
        """
        Take the specified number of last characters in a string

        :param column: column whose contents to use
        :param length: number of characters to take from right
        :return: a concat automapper type
        """
        return AutoMapperSubstringDataType(
            column=column, start=-length, length=length
        )

    @staticmethod
    def substring(
        column: AutoMapperColumnOrColumnLikeType, start: int, length: int
    ) -> AutoMapperSubstringDataType:
        """
        Finds a substring in the specified string.

        :param column: column whose contents to use
        :param start: position to start
        :param length: number of characters to take
        :return: a concat automapper type
        """
        return AutoMapperSubstringDataType(
            column=column, start=start, length=length
        )

    @staticmethod
    def string_before_delimiter(
        column: AutoMapperColumnOrColumnLikeType, delimiter: str
    ) -> AutoMapperSubstringByDelimiterDataType:
        """
        Take the specified number of first characters in a string

        :param column: column whose contents to use
        :param delimiter: string to use as delimiter
        :return: a concat automapper type
        """
        return AutoMapperSubstringByDelimiterDataType(
            column=column, delimiter=delimiter, delimiter_count=1
        )

    @staticmethod
    def string_after_delimiter(
        column: AutoMapperColumnOrColumnLikeType, delimiter: str
    ) -> AutoMapperSubstringByDelimiterDataType:
        """
        Take the specified number of first characters in a string

        :param column: column whose contents to use
        :param delimiter: string to use as delimiter
        :return: a concat automapper type
        """
        return AutoMapperSubstringByDelimiterDataType(
            column=column, delimiter=delimiter, delimiter_count=-1
        )

    @staticmethod
    def substring_by_delimiter(
        column: AutoMapperColumnOrColumnLikeType, delimiter: str,
        delimiter_count: int
    ) -> AutoMapperSubstringByDelimiterDataType:
        """
        Returns the substring from string str before count occurrences of the delimiter.
        substring_by_delimiter performs a case-sensitive match when searching for delimiter.

        :param column: column whose contents to use
        :param delimiter: string to use as delimiter.  can be a regex.
        :param delimiter_count: If delimiter_count is positive, everything the left of the final delimiter
                                    (counting from left) is returned.
                                If delimiter_count is negative, every to the right of the final delimiter
                                    (counting from the right) is returned.
        :return: a concat automapper type
        """
        return AutoMapperSubstringByDelimiterDataType(
            column=column,
            delimiter=delimiter,
            delimiter_count=delimiter_count
        )

    @staticmethod
    def regex_replace(
        column: AutoMapperColumnOrColumnLikeType, pattern: str,
        replacement: str
    ) -> AutoMapperRegExReplaceDataType:
        """
        Replace all substrings of the specified string value that match regexp with rep.

        :param column: column whose contents to replace
        :param pattern: pattern to search for
        :param replacement: string to replace with
        :return: a regex_replace automapper type
        """
        return AutoMapperRegExReplaceDataType(
            column=column, pattern=pattern, replacement=replacement
        )

    @staticmethod
    def trim(
        column: AutoMapperColumnOrColumnLikeType
    ) -> AutoMapperTrimDataType:
        """
        Trim the spaces from both ends for the specified string column.

        :param column: column whose contents to trim
        :return: a trim automapper type
        """
        return AutoMapperTrimDataType(column=column)

    @staticmethod
    def hash(
        *args: Union[AutoMapperNativeTextType, AutoMapperWrapperType,
                     AutoMapperTextLikeBase]
    ) -> AutoMapperHashDataType:
        """
        Calculates the hash code of given columns, and returns the result as an int column.


        :param args: string or column
        :return: a concat automapper type
        """
        return AutoMapperHashDataType(*args)

    @staticmethod
    def coalesce(*args: _TAutoMapperDataType) -> _TAutoMapperDataType:
        """
        Returns the first value that is not null.

        :return: a coalesce automapper type
        """

        # cast it to the inner type so type checking is happy
        return cast(_TAutoMapperDataType, AutoMapperCoalesceDataType(*args))

    @staticmethod
    def if_regex(
        column: AutoMapperColumnOrColumnLikeType,
        check: Union[str, List[str]],
        value: _TAutoMapperDataType,
        else_: Optional[_TAutoMapperDataType] = None
    ) -> _TAutoMapperDataType:
        """
        Checks if column matches check_value.  Returns value if it matches else else_


        :param column: column to check
        :param check: value to compare the column to. Has to be a string or list of strings
        :param value: what to return if the value matches
        :param else_: what value to assign if check fails
        :return: an if automapper type
        """

        # cast it to the inner type so type checking is happy
        return cast(
            _TAutoMapperDataType,
            AutoMapperIfRegExDataType(
                column=column, check=check, value=value, else_=else_
            )
        )

    @staticmethod
    def filter(
        column: AutoMapperColumnOrColumnLikeType,
        func: Callable[[Dict[str, Any]], Any]
    ) -> AutoMapperFilterDataType:
        """
        Filters a column by a function


        :param column: column to check
        :param func: func to filter by
        :return: a filter automapper type
        """
        # # cast it to the inner type so type checking is happy
        # return cast(
        #     _TAutoMapperDataType,
        #     AutoMapperFilterDataType(column=column, func=func)
        # )
        return AutoMapperFilterDataType(column=column, func=func)

    @staticmethod
    def transform(
        column: AutoMapperColumnOrColumnLikeType, value: _TAutoMapperDataType
    ) -> List[_TAutoMapperDataType]:
        """
        transforms a column into another type or struct


        :param column: column to check
        :param value: func to create type or struct
        :return: a transform automapper type
        """
        # cast it to the inner type so type checking is happy
        return cast(
            List[_TAutoMapperDataType],
            AutoMapperTransformDataType(column=column, value=value)
        )

    @staticmethod
    def field(value: str) -> AutoMapperTextLikeBase:
        """
        Specifies that the value parameter should be used as a field name
        :param value: name of column
        :return: A column automapper type
        """
        return AutoMapperDataTypeField(value)

    @staticmethod
    def current() -> AutoMapperTextLikeBase:
        """
        Specifies to use the current item
        :return: A column automapper type
        """
        return AutoMapperDataTypeField("_")

    @staticmethod
    def split_by_delimiter(
        column: AutoMapperColumnOrColumnLikeType, delimiter: str
    ) -> AutoMapperSplitByDelimiterDataType:
        """
        Split a string into an array using the delimiter

        :param column: column whose contents to use
        :param delimiter: string to use as delimiter
        :return: a concat automapper type
        """
        return AutoMapperSplitByDelimiterDataType(
            column=column, delimiter=delimiter
        )

    @staticmethod
    def float(value: AutoMapperDataTypeBase) -> 'AutoMapperDataTypeBase':
        """
        Converts column to float

        :return:
        :rtype:
        """
        from spark_auto_mapper.data_types.float import AutoMapperFloatDataType

        return cast(
            'AutoMapperDataTypeBase', AutoMapperFloatDataType(value=value)
        )

    @staticmethod
    def flatten(
        column: AutoMapperColumnOrColumnLikeType
    ) -> "AutoMapperDataTypeBase":
        """
        creates a single array from an array of arrays.
        If a structure of nested arrays is deeper than two levels, only one level of nesting is removed.
        source: http://spark.apache.org/docs/latest/api/python/_modules/pyspark/sql/functions.html#flatten

        :return: a flatten automapper type
        """
        from spark_auto_mapper.data_types.flatten import AutoMapperFlattenDataType

        # cast it to the inner type so type checking is happy
        return cast(
            "AutoMapperDataTypeBase", AutoMapperFlattenDataType(column=column)
        )

    @staticmethod
    def array(value: AutoMapperDataTypeBase) -> "AutoMapperDataTypeBase":
        """
        creates an array from a single item.
        source: http://spark.apache.org/docs/latest/api/python/_modules/pyspark/sql/functions.html#array

        :return: an array automapper type
        """
        from spark_auto_mapper.data_types.array import AutoMapperArrayDataType

        # cast it to the inner type so type checking is happy
        return cast(
            "AutoMapperDataTypeBase", AutoMapperArrayDataType(value=value)
        )

    @staticmethod
    def join_using_delimiter(
        column: AutoMapperColumnOrColumnLikeType, delimiter: str
    ) -> AutoMapperJoinUsingDelimiterDataType:
        """
        Joins an array and forms a string using the delimiter
        :param column: column whose contents to use
        :param delimiter: string to use as delimiter
        :return: a join automapper type
        """
        return AutoMapperJoinUsingDelimiterDataType(
            column=column, delimiter=delimiter
        )
