## TiDB Cloud Lake UDF Server SDK
This library provides a SDK for creating user-defined functions (UDF) servers for TiDB Cloud Lake.

[![Python](https://img.shields.io/pypi/v/lake-udf)](https://pypi.org/project/lake-udf/)

### Introduction
TiDB Cloud Lake supports user-defined functions implemented as external functions. With the TiDB Cloud Lake Python UDF API, users can define custom UDFs using Python and start a Python process as a UDF server. Then users can call the customized UDFs in TiDB Cloud Lake. TiDB Cloud Lake will remotely access the UDF server to execute the defined functions.
