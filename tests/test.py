from flatten.hive_parser import HiveParser


def test_nested_struct():
    hive_str = r"""
    struct<loc_lat:double,service_handler:string,ip_address:string,device:bigint,source:struct<id:string,contacts:
    struct<admin:struct<email:string,name:array<string>>>,name:string>,loc_name:string,tags:array<
    struct<key:string,value:string>>>
    """
    hive = HiveParser()
    data = hive(hive_str)
    assert data == {
        "loc_lat": "double",
        "service_handler": "string",
        "ip_address": "string",
        "device": "bigint",
        "source": {
            "id": "string",
            "contacts": {"admin": {"email": "string", "name": ["string"]}},
            "name": "string",
        },
        "loc_name": "string",
        "tags": [{"key": "string", "value": "string"}],
    }


def test_decimal_precision():
    hive = HiveParser()

    hive_str2 = r"""
    struct<
    loc_lat:decimal(12,1),
    service_handler:string,
    ip_address:string,
    device:bigint,
    source:struct<id:string,
                contacts:struct<
                admin:struct<email:string,
                name:array<string>>>,
                name:string>,
                loc_name:string,
                tags:array<
                struct<key:string,
                value:string>>>
    """
    data2 = hive(hive_str2)
    assert data2 == {
        "loc_lat": "decimal(12,1)",
        "service_handler": "string",
        "ip_address": "string",
        "device": "bigint",
        "source": {
            "id": "string",
            "contacts": {"admin": {"email": "string", "name": ["string"]}},
            "name": "string",
        },
        "loc_name": "string",
        "tags": [{"key": "string", "value": "string"}],
    }


def test_primitive_type():
    hive = HiveParser()

    hive_str3 = r"""
    string
        """
    data3 = hive(hive_str3)
    assert data3 == "string"


def test_struct():
    hive = HiveParser()
    hive_str4 = r"""
    struct<
    loc_lat:decimal,
    service_handler:string
    >
    """
    data4 = hive(hive_str4)
    assert data4 == {"loc_lat": "decimal", "service_handler": "string"}
