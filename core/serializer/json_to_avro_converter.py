"""
JSON to Avro schema converter and data mapper.

This module provides functionality to:
1. Infer Avro schemas from JSON data
2. Convert JSON data to Avro-compatible format
3. Validate JSON against Avro schemas
"""

import json
from typing import Any, Optional
from datetime import datetime
from decimal import Decimal


class JsonToAvroConverter:
    """
    Converts JSON data to Avro format and infers schemas.
    
    This class handles the conversion between JSON and Avro data formats,
    including type inference and schema generation.
    """
    
    # Type mapping from Python/JSON to Avro
    TYPE_MAPPING = {
        str: "string",
        int: "long",
        float: "double",
        bool: "boolean",
        type(None): "null",
        list: "array",
        dict: "record",
        bytes: "bytes",
    }
    
    @staticmethod
    def infer_avro_type(value: Any) -> str | dict | list:
        """
        Infer Avro type from a Python value.
        
        Args:
            value: Python value to infer type from
            
        Returns:
            Avro type specification (string or complex type dict)
        """
        if value is None:
            return "null"
        
        value_type = type(value)
        
        if value_type in [str, int, float, bool, bytes]:
            # Handle special cases for numbers
            if isinstance(value, int):
                # Use int for small numbers, long for large
                return "int" if -2147483648 <= value <= 2147483647 else "long"
            return JsonToAvroConverter.TYPE_MAPPING.get(value_type, "string")
        
        elif isinstance(value, list):
            if not value:
                # Empty array - default to string items
                return {"type": "array", "items": "string"}
            
            # Infer type from first element (assuming homogeneous arrays)
            item_type = JsonToAvroConverter.infer_avro_type(value[0])
            return {"type": "array", "items": item_type}
        
        elif isinstance(value, dict):
            # For nested objects, create a record type
            fields = []
            for key, val in value.items():
                field_type = JsonToAvroConverter.infer_avro_type(val)
                fields.append({
                    "name": key,
                    "type": ["null", field_type] if val is not None else "null",
                    "default": None
                })
            return {
                "type": "record",
                "name": "NestedRecord",
                "fields": fields
            }
        
        # Default to string for unknown types
        return "string"
    
    @staticmethod
    def infer_schema(
        data: dict[str, Any],
        schema_name: str = "InferredSchema",
        namespace: str = "com.feed.avro"
    ) -> dict:
        """
        Infer an Avro schema from JSON data.
        
        Args:
            data: JSON data dictionary
            schema_name: Name for the schema
            namespace: Namespace for the schema
            
        Returns:
            Avro schema as a dictionary
        """
        fields = []
        
        for key, value in data.items():
            avro_type = JsonToAvroConverter.infer_avro_type(value)
            
            # Make fields nullable by default
            if value is not None and avro_type != "null":
                field_type = ["null", avro_type]
                default = None
            else:
                field_type = "null"
                default = None
            
            fields.append({
                "name": key,
                "type": field_type,
                "default": default
            })
        
        schema = {
            "type": "record",
            "name": schema_name,
            "namespace": namespace,
            "fields": fields
        }
        
        return schema
    
    @staticmethod
    def convert_value(value: Any, avro_type: str | dict | list) -> Any:
        """
        Convert a Python value to Avro-compatible format.
        
        Args:
            value: Python value to convert
            avro_type: Target Avro type
            
        Returns:
            Avro-compatible value
        """
        if value is None:
            return None
        
        # Handle union types (e.g., ["null", "string"])
        if isinstance(avro_type, list):
            # Find the non-null type
            for t in avro_type:
                if t != "null":
                    return JsonToAvroConverter.convert_value(value, t)
            return None
        
        # Handle complex types
        if isinstance(avro_type, dict):
            avro_type_name = avro_type.get("type")
            
            if avro_type_name == "array":
                if not isinstance(value, list):
                    value = [value]
                items_type = avro_type.get("items")
                return [
                    JsonToAvroConverter.convert_value(item, items_type)
                    for item in value
                ]
            
            elif avro_type_name == "record":
                if not isinstance(value, dict):
                    return value
                
                result = {}
                fields = avro_type.get("fields", [])
                for field in fields:
                    field_name = field["name"]
                    field_type = field["type"]
                    field_value = value.get(field_name)
                    result[field_name] = JsonToAvroConverter.convert_value(
                        field_value, field_type
                    )
                return result
            
            elif avro_type_name == "map":
                values_type = avro_type.get("values")
                return {
                    k: JsonToAvroConverter.convert_value(v, values_type)
                    for k, v in value.items()
                }
        
        # Handle primitive types
        if avro_type in ["int", "long"]:
            return int(value) if value is not None else None
        elif avro_type == "float" or avro_type == "double":
            return float(value) if value is not None else None
        elif avro_type == "boolean":
            return bool(value) if value is not None else None
        elif avro_type == "string":
            return str(value) if value is not None else None
        elif avro_type == "bytes":
            if isinstance(value, str):
                return value.encode('utf-8')
            return value
        
        return value
    
    @staticmethod
    def json_to_avro(
        json_data: dict[str, Any],
        schema: dict
    ) -> dict[str, Any]:
        """
        Convert JSON data to Avro format using a schema.
        
        Args:
            json_data: Input JSON data
            schema: Avro schema to use for conversion
            
        Returns:
            Avro-formatted data
        """
        if schema.get("type") != "record":
            raise ValueError("Schema must be a record type")
        
        result = {}
        fields = schema.get("fields", [])
        
        for field in fields:
            field_name = field["name"]
            field_type = field["type"]
            field_value = json_data.get(field_name)
            
            # Use default value if field is missing
            if field_value is None and "default" in field:
                field_value = field["default"]
            
            result[field_name] = JsonToAvroConverter.convert_value(
                field_value, field_type
            )
        
        return result
    
    @staticmethod
    def validate_against_schema(
        data: dict[str, Any],
        schema: dict
    ) -> tuple[bool, Optional[str]]:
        """
        Validate JSON data against an Avro schema.
        
        Args:
            data: Data to validate
            schema: Avro schema
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            if schema.get("type") != "record":
                return False, "Schema must be a record type"
            
            fields = schema.get("fields", [])
            schema_fields = {f["name"] for f in fields}
            required_fields = {
                f["name"] for f in fields 
                if "default" not in f and "null" not in str(f.get("type"))
            }
            
            data_fields = set(data.keys())
            
            # Check for missing required fields
            missing_fields = required_fields - data_fields
            if missing_fields:
                return False, f"Missing required fields: {missing_fields}"
            
            # Validate each field type
            for field in fields:
                field_name = field["name"]
                field_type = field["type"]
                
                if field_name in data:
                    value = data[field_name]
                    if not JsonToAvroConverter._validate_type(value, field_type):
                        return False, f"Invalid type for field '{field_name}'"
            
            return True, None
            
        except Exception as e:
            return False, str(e)
    
    @staticmethod
    def _validate_type(value: Any, avro_type: str | dict | list) -> bool:
        """
        Validate that a value matches an Avro type.
        
        Args:
            value: Value to validate
            avro_type: Avro type specification
            
        Returns:
            True if valid, False otherwise
        """
        if value is None:
            if isinstance(avro_type, list):
                return "null" in avro_type
            return avro_type == "null"
        
        if isinstance(avro_type, list):
            # Union type - check if value matches any type
            return any(
                JsonToAvroConverter._validate_type(value, t)
                for t in avro_type if t != "null"
            )
        
        if isinstance(avro_type, dict):
            type_name = avro_type.get("type")
            if type_name == "array":
                return isinstance(value, list)
            elif type_name == "record":
                return isinstance(value, dict)
            elif type_name == "map":
                return isinstance(value, dict)
        
        # Primitive type checking
        type_checks = {
            "string": lambda v: isinstance(v, str),
            "int": lambda v: isinstance(v, int) and -2147483648 <= v <= 2147483647,
            "long": lambda v: isinstance(v, int),
            "float": lambda v: isinstance(v, (int, float)),
            "double": lambda v: isinstance(v, (int, float)),
            "boolean": lambda v: isinstance(v, bool),
            "bytes": lambda v: isinstance(v, (bytes, bytearray)),
            "null": lambda v: v is None,
        }
        
        check = type_checks.get(avro_type)
        return check(value) if check else False
    
    @staticmethod
    def schema_to_json_string(schema: dict) -> str:
        """
        Convert Avro schema dict to JSON string.
        
        Args:
            schema: Avro schema dictionary
            
        Returns:
            JSON string representation
        """
        return json.dumps(schema, indent=2)
    
    @staticmethod
    def schema_from_json_string(schema_str: str) -> dict:
        """
        Parse Avro schema from JSON string.
        
        Args:
            schema_str: JSON string representation of schema
            
        Returns:
            Avro schema dictionary
        """
        return json.loads(schema_str)
