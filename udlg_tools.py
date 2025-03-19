import csv as csv_module
import json
import struct
from enum import Enum
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional

import typer
from rich import print


class PrimitiveType(Enum):
    Boolean = 1
    Byte = 2
    Char = 3
    Decimal = 5
    Double = 6
    Int16 = 7
    Int32 = 8
    Int64 = 9
    SByte = 10
    Single = 11
    TimeSpan = 12
    DateTime = 13
    UInt16 = 14
    UInt32 = 15
    UInt64 = 16
    Null = 17
    String = 18


class BinaryType(Enum):
    Primitive = 0
    String = 1
    Object = 2
    SystemClass = 3
    Class = 4
    ObjectArray = 5
    StringArray = 6
    PrimitiveArray = 7


class BinaryArrayType(Enum):
    Single = 0
    Jagged = 1
    Rectangular = 2
    SingleOffset = 3
    JaggedOffset = 4
    RectangularOffset = 5


class RecordType(Enum):
    SerializedStreamHeader = 0
    ClassWithId = 1
    SystemClassWithMembers = 2
    ClassWithMembers = 3
    SystemClassWithMembersAndTypes = 4
    ClassWithMembersAndTypes = 5
    BinaryObjectString = 6
    BinaryArray = 7
    MemberPrimitiveTyped = 8
    MemberReference = 9
    ObjectNull = 10
    MessageEnd = 11
    BinaryLibrary = 12
    ObjectNullMultiple256 = 13
    ObjectNullMultiple = 14
    ArraySinglePrimitive = 15
    ArraySingleObject = 16
    ArraySingleString = 17
    MethodCall = 21
    MethodReturn = 22


class NetSerializer:
    """
    Serializer/Deserializer for UDLG (or similar) binary format.
    Handles recording objects and references, reading and writing
    data primitives, arrays, classes, etc.
    """

    def __init__(self, stream: BytesIO):
        self.stream = stream
        self.objects: Dict[int, Dict[str, Any]] = {}
        self.references: List[Dict[str, Any]] = []
        self.records: List[Dict[str, Any]] = []

    def get_element_by_object_id(self, target_id: int, data: Any = None):
        """Return the element (dict) in 'data' that matches a given ObjectId."""
        if data is None:
            data = self.records
        if isinstance(data, dict):
            if data.get("ObjectId") == target_id:
                return data
            for value in data.values():
                result = self.get_element_by_object_id(target_id, value)
                if result is not None:
                    return result
        elif isinstance(data, list):
            for item in data:
                result = self.get_element_by_object_id(target_id, item)
                if result is not None:
                    return result
        return None

    def get_parent_by_object_id(self, target_id: int, data: Any, parent: Any = None):
        """Return the parent of the element (dict) that matches a given ObjectId."""
        if isinstance(data, dict):
            if data.get("ObjectId") == target_id:
                return parent
            for key, value in data.items():
                result = self.get_parent_by_object_id(target_id, value, data)
                if result is not None:
                    return result
        elif isinstance(data, list):
            for item in data:
                result = self.get_parent_by_object_id(target_id, item, data)
                if result is not None:
                    return result
        return None

    def read(self, size: int) -> bytes:
        """Read a fixed number of bytes from the stream."""
        data = self.stream.read(size)
        if len(data) != size:
            raise EOFError("Unexpected end of stream while reading.")
        return data

    def write(self, data: bytes) -> None:
        """Write raw bytes to the stream."""
        self.stream.write(data)

    def read_write_primitive(
        self,
        primitive_type: PrimitiveType,
        value: Any = None,
        mode: str = "read",
    ) -> Any:
        """
        Read or write a primitive value based on the specified PrimitiveType.
        mode: "read" or "write".
        """
        type_map = {
            PrimitiveType.Boolean: ("<?", 1),
            PrimitiveType.Byte: ("<B", 1),
            PrimitiveType.SByte: ("<b", 1),
            PrimitiveType.Int16: ("<h", 2),
            PrimitiveType.UInt16: ("<H", 2),
            PrimitiveType.Int32: ("<i", 4),
            PrimitiveType.UInt32: ("<I", 4),
            PrimitiveType.Int64: ("<q", 8),
            PrimitiveType.UInt64: ("<Q", 8),
            PrimitiveType.Single: ("<f", 4),
            PrimitiveType.Double: ("<d", 8),
        }

        if primitive_type in type_map:
            fmt, size = type_map[primitive_type]
            if mode == "read":
                return struct.unpack(fmt, self.read(size))[0]
            else:
                self.write(struct.pack(fmt, value))

        elif primitive_type == PrimitiveType.Char:
            return self.read_write_string(value, mode)
        elif primitive_type == PrimitiveType.String:
            return self.read_write_string(value, mode)
        elif primitive_type == PrimitiveType.Decimal:
            return self.read_write_decimal(value, mode)
        elif primitive_type == PrimitiveType.DateTime:
            return self.read_write_datetime(value, mode)
        elif primitive_type == PrimitiveType.TimeSpan:
            # TimeSpan is stored as Int64
            return self.read_write_primitive(PrimitiveType.Int64, value, mode)
        else:
            raise ValueError(f"Unsupported primitive type: {primitive_type}")

    def read_write_string(self, value: Optional[str] = None, mode: str = "read") -> Any:
        """Read or write a string with 7-bit length encoding."""
        if mode == "read":
            length = self.read_write_7bit_encoded_int()
            return self.read(length).decode("utf-8")
        else:
            encoded = value.encode("utf-8") if value else b""
            self.read_write_7bit_encoded_int(len(encoded), mode="write")
            self.write(encoded)

    def read_write_7bit_encoded_int(
        self, value: Optional[int] = None, mode: str = "read"
    ) -> Any:
        """
        Read or write a 7-bit encoded integer, often used in .NET serialization.
        """
        if mode == "read":
            result = 0
            shift = 0
            while True:
                byte_val = self.read_write_primitive(PrimitiveType.Byte)
                result |= (byte_val & 0x7F) << shift
                if not byte_val & 0x80:
                    return result
                shift += 7
                if shift >= 35:
                    raise ValueError("Invalid 7-bit encoded int, shift too large.")
        else:
            if value is None:
                value = 0
            if value < 0:
                raise ValueError("Value must be non-negative for 7-bit encoding.")

            # Special case: zero
            if value == 0:
                self.read_write_primitive(PrimitiveType.Byte, 0, mode="write")
                return

            while value > 0:
                byte_val = value & 0x7F
                value >>= 7
                if value > 0:
                    byte_val |= 0x80
                self.read_write_primitive(PrimitiveType.Byte, byte_val, mode="write")

    def read_write_decimal(self, value: Optional[float] = None, mode: str = "read"):
        """Handle decimal as a string, as a simplistic approach."""
        if mode == "read":
            return self.read_write_string(mode="read")
        else:
            return self.read_write_string(
                str(value) if value is not None else "", "write"
            )

    def read_write_datetime(
        self, value: Optional[Dict[str, Any]] = None, mode: str = "read"
    ):
        """
        Read or write a DateTime value in a .NET-like format:
        - The last two bits store DateTimeKind: 0 = Unspecified, 1 = UTC, 2 = Local.
        - The remaining bits store the Ticks value.
        """
        if mode == "read":
            ticks = self.read_write_primitive(PrimitiveType.Int64)
            return {
                "Kind": "UTC" if ticks & 0x01 else "Local" if ticks & 0x02 else None,
                "ticks": ticks & ~0x03,
            }
        else:
            kind_str = value.get("Kind", None)
            ticks_val = value.get("ticks", 0)
            # 1 => UTC, 2 => Local, 0 => unspecified
            kind = 1 if kind_str == "UTC" else (2 if kind_str == "Local" else 0)
            self.read_write_primitive(
                PrimitiveType.Int64, ticks_val | kind, mode="write"
            )

    def read_write_enum(self, enum_class: Enum, value: Any = None, mode: str = "read"):
        """Generic enum read/write. It uses one byte to store the enum value."""
        if mode == "read":
            return enum_class(self.read_write_primitive(PrimitiveType.Byte))
        else:
            self.read_write_primitive(PrimitiveType.Byte, value.value, mode="write")

    def read_write_class_type_info(
        self, value: Dict[str, Any] = None, mode: str = "read"
    ):
        """Handle reading/writing of class type info structures."""
        if mode == "read":
            return {
                "TypeName": self.read_write_string(mode="read"),
                "LibraryId": self.read_write_primitive(
                    PrimitiveType.Int32, mode="read"
                ),
            }
        else:
            self.read_write_string(value["TypeName"], mode="write")
            self.read_write_primitive(
                PrimitiveType.Int32, value["LibraryId"], mode="write"
            )

    def read_write_class_info(self, value: Dict[str, Any] = None, mode: str = "read"):
        """Handle reading/writing the initial class info, such as name, ID, etc."""
        object_id = self.read_write_primitive(
            PrimitiveType.Int32,
            None if mode == "read" else value["ObjectId"],
            mode,
        )
        name = self.read_write_string(
            None if mode == "read" else value["Name"],
            mode,
        )
        member_count = self.read_write_primitive(
            PrimitiveType.Int32,
            None if mode == "read" else value["MemberCount"],
            mode,
        )
        if mode == "read":
            return {
                "ObjectId": object_id,
                "Name": name,
                "MemberCount": member_count,
                "MemberNames": [
                    self.read_write_string(mode="read") for _ in range(member_count)
                ],
            }
        else:
            for m_name in value["MemberNames"]:
                self.read_write_string(m_name, mode="write")

    def read_write_member_type_info(
        self,
        count: Optional[int] = None,
        value: Dict[str, Any] = None,
        mode: str = "read",
    ):
        """Read or write the type info for each member in a class."""
        if mode == "read":
            btypes = [
                self.read_write_enum(BinaryType, mode="read") for _ in range(count)
            ]
            return {
                "BinaryTypeEnums": [btype.name for btype in btypes],
                "AdditionalInfos": [
                    self.read_write_binary_type_info(btype, mode="read")
                    for btype in btypes
                ],
            }
        else:
            for btype_name in value["BinaryTypeEnums"]:
                self.read_write_enum(BinaryType, BinaryType[btype_name], mode="write")
            for i, info in enumerate(value["AdditionalInfos"]):
                binary_type = BinaryType[value["BinaryTypeEnums"][i]]
                self.read_write_binary_type_info(binary_type, info, mode="write")

    def read_write_binary_type_info(
        self,
        binary_type: BinaryType,
        value: Any = None,
        mode: str = "read",
    ):
        """Dispatcher for additional type-specific info for certain binary types."""
        if binary_type in (BinaryType.Primitive, BinaryType.PrimitiveArray):
            if mode == "read":
                pt_enum = self.read_write_enum(PrimitiveType, mode="read")
                return pt_enum.name
            else:
                self.read_write_enum(PrimitiveType, PrimitiveType[value], mode="write")
        elif binary_type == BinaryType.SystemClass:
            return self.read_write_string(value, mode)
        elif binary_type == BinaryType.Class:
            return self.read_write_class_type_info(value, mode)
        elif binary_type in (
            BinaryType.String,
            BinaryType.StringArray,
            BinaryType.Object,
        ):
            return None
        else:
            raise ValueError(f"Unexpected binary_type: {binary_type}")

    def read_write_array_info(self, value: Dict[str, Any] = None, mode: str = "read"):
        """Read or write basic Array info containing ObjectId and array length."""
        object_id = self.read_write_primitive(
            PrimitiveType.Int32,
            None if mode == "read" else value["ObjectId"],
            mode,
        )
        length = self.read_write_primitive(
            PrimitiveType.Int32,
            None if mode == "read" else value["Length"],
            mode,
        )
        if mode == "read":
            return {"ObjectId": object_id, "Length": length}

    def read_write_record(
        self,
        record: Optional[Dict[str, Any]] = None,
        mode: str = "read",
        records: Optional[List[Dict[str, Any]]] = None,
    ):
        """
        High-level dispatcher for reading or writing record types.
        It reads/writes the RecordType and passes control to process_record.
        """
        if records is not None:
            self.records = records
        if mode == "read":
            record_type = self.read_write_enum(RecordType, mode="read")
            return self.process_record(record_type, records=self.records)
        else:
            record_type = RecordType[record["RecordTypeEnum"]]
            self.read_write_enum(RecordType, record_type, mode="write")
            self.process_record(record_type, record, mode="write", records=self.records)

    def process_record(
        self,
        record_type: RecordType,
        record: Optional[Dict[str, Any]] = None,
        mode: str = "read",
        records: Optional[List[Dict[str, Any]]] = None,
    ):
        """
        Process a record after reading the record type. Dispatches to specific
        handler methods based on the RecordType enum.
        """
        handlers = {
            RecordType.SerializedStreamHeader: self.handle_serialized_stream_header,
            RecordType.ClassWithId: self.handle_class_with_id,
            RecordType.SystemClassWithMembers: self.handle_system_class_with_members,
            RecordType.ClassWithMembers: self.handle_class_with_members,
            RecordType.SystemClassWithMembersAndTypes: self.handle_system_class_with_members_and_types,
            RecordType.ClassWithMembersAndTypes: self.handle_class_with_members_and_types,
            RecordType.BinaryObjectString: self.handle_binary_object_string,
            RecordType.BinaryArray: self.handle_binary_array,
            RecordType.MemberPrimitiveTyped: self.handle_member_primitive_typed,
            RecordType.MemberReference: self.handle_member_reference,
            RecordType.ObjectNull: self.handle_object_null,
            RecordType.MessageEnd: self.handle_message_end,
            RecordType.BinaryLibrary: self.handle_binary_library,
            RecordType.ObjectNullMultiple256: self.handle_object_null_multiple_256,
            RecordType.ObjectNullMultiple: self.handle_object_null_multiple,
            RecordType.ArraySinglePrimitive: self.handle_array_single_primitive,
            RecordType.ArraySingleObject: self.handle_array_single_object,
            RecordType.ArraySingleString: self.handle_array_single_string,
        }

        if records is not None:
            self.records = records
        handler = handlers.get(record_type)
        if handler:
            result = handler(record, mode=mode)
            if record is None and isinstance(result, dict):
                result["RecordTypeEnum"] = record_type.name
            return result
        else:
            raise ValueError(f"Unsupported record type: {record_type}")

    def handle_serialized_stream_header(self, record=None, mode="read"):
        """Handle the overall stream header (root, version, etc.)."""
        root_id = self.read_write_primitive(
            PrimitiveType.Int32,
            None if mode == "read" else record["RootId"],
            mode,
        )
        header_id = self.read_write_primitive(
            PrimitiveType.Int32,
            None if mode == "read" else record["HeaderId"],
            mode,
        )
        major_version = self.read_write_primitive(
            PrimitiveType.Int32,
            None if mode == "read" else record["MajorVersion"],
            mode,
        )
        minor_version = self.read_write_primitive(
            PrimitiveType.Int32,
            None if mode == "read" else record["MinorVersion"],
            mode,
        )
        if mode == "read":
            return {
                "RootId": root_id,
                "HeaderId": header_id,
                "MajorVersion": major_version,
                "MinorVersion": minor_version,
            }

    def handle_binary_object_string(self, record=None, mode="read"):
        """Handle a string record that contains an ObjectId and the string value."""
        obj_id = self.read_write_primitive(
            PrimitiveType.Int32,
            None if mode == "read" else record["ObjectId"],
            mode,
        )
        value = self.read_write_string(
            None if mode == "read" else record["Value"], mode
        )
        if mode == "read":
            return {"ObjectId": obj_id, "Value": value}

    def handle_class_with_id(self, record=None, mode="read"):
        """Handle a class record that references metadata by ID."""
        obj_id = self.read_write_primitive(
            PrimitiveType.Int32,
            None if mode == "read" else record["ObjectId"],
            mode,
        )
        metadata_id = self.read_write_primitive(
            PrimitiveType.Int32,
            None if mode == "read" else record["MetadataId"],
            mode,
        )
        if mode == "read":
            record_data = {"ObjectId": obj_id, "MetadataId": metadata_id}
            self.read_write_class_values(record_data)
            return record_data
        else:
            self.read_write_class_values(record, record, mode="write")

    def handle_system_class_with_members(self, record=None, mode="read"):
        """Handle a system class record with members, but no type info for members."""
        class_info = self.read_write_class_info(
            None if mode == "read" else record["ClassInfo"],
            mode,
        )
        if mode == "read":
            return {"ClassInfo": class_info}

    def handle_class_with_members(self, record=None, mode="read"):
        """Handle a normal class record with members, but no explicit type info."""
        class_info = self.read_write_class_info(
            None if mode == "read" else record["ClassInfo"],
            mode,
        )
        library_id = self.read_write_primitive(
            PrimitiveType.Int32,
            None if mode == "read" else record["LibraryId"],
            mode,
        )
        if mode == "read":
            return {"ClassInfo": class_info, "LibraryId": library_id}

    def handle_system_class_with_members_and_types(self, record=None, mode="read"):
        """Handle a system class record that also has explicit type info for each member."""
        return self.handle_class_with_members_and_types(record, system=True, mode=mode)

    def handle_class_with_members_and_types(
        self, record=None, system=False, mode="read"
    ):
        """Handle a class record with explicit type info for each member."""
        class_info = self.read_write_class_info(
            None if mode == "read" else record["ClassInfo"],
            mode,
        )
        if mode == "read":
            member_type_info = self.read_write_member_type_info(
                class_info["MemberCount"], mode="read"
            )

            record_data = {
                "ClassInfo": class_info,
                "MemberTypeInfo": member_type_info,
            }
            if not system:
                record_data["LibraryId"] = self.read_write_primitive(
                    PrimitiveType.Int32, mode="read"
                )

            # We append a temporary record so we can reference it:
            self.records.append({"__temp_record": record_data})

            self.read_write_class_values(record_data)
            return record_data
        else:
            # Write
            self.read_write_member_type_info(
                record["ClassInfo"]["MemberCount"],
                record["MemberTypeInfo"],
                mode="write",
            )
            if not system:
                self.read_write_primitive(
                    PrimitiveType.Int32, record["LibraryId"], mode="write"
                )
            self.read_write_class_values(record, record, mode="write")

    def handle_binary_array(self, record=None, mode="read"):
        """Handle a multi-element array record."""
        if mode == "read":
            obj_id = self.read_write_primitive(PrimitiveType.Int32, mode="read")
            binary_array_type = self.read_write_enum(BinaryArrayType, mode="read")
            rank = self.read_write_primitive(PrimitiveType.Int32, mode="read")
            lengths = [
                self.read_write_primitive(PrimitiveType.Int32, mode="read")
                for _ in range(rank)
            ]

            # If the array type includes offsets, read them too
            bounds = (
                [
                    self.read_write_primitive(PrimitiveType.Int32, mode="read")
                    for _ in range(rank)
                ]
                if binary_array_type.name.endswith("Offset")
                else []
            )

            binary_type = self.read_write_enum(BinaryType, mode="read")
            additional_type_info = self.read_write_binary_type_info(
                binary_type, mode="read"
            )

            record_data = {
                "ObjectId": obj_id,
                "BinaryArrayTypeEnum": binary_array_type.name,
                "rank": rank,
                "Lengths": lengths,
                "LowerBounds": bounds,
                "TypeEnum": binary_type.name,
                "AdditionalTypeInfo": additional_type_info,
            }

            if binary_array_type.name.endswith("Offset") or rank > 1:
                raise NotImplementedError(
                    f"BinaryArray of type {binary_array_type.name} with rank {rank} not implemented."
                )

            # Read array elements
            cells = lengths[0]
            values = []
            i = 0
            while i < cells:
                value = self.read_write_record(mode="read", records=self.records)
                if isinstance(value, dict) and "NullCount" in value:
                    i += value["NullCount"]
                else:
                    i += 1
                if i > cells:
                    raise ValueError("Too many records in array.")
                values.append(value)

            record_data["Values"] = values
            return record_data
        else:
            # Write
            self.read_write_primitive(
                PrimitiveType.Int32, record["ObjectId"], mode="write"
            )
            self.read_write_enum(
                BinaryArrayType,
                BinaryArrayType[record["BinaryArrayTypeEnum"]],
                mode="write",
            )
            self.read_write_primitive(PrimitiveType.Int32, record["rank"], mode="write")
            for length in record["Lengths"]:
                self.read_write_primitive(PrimitiveType.Int32, length, mode="write")

            if BinaryArrayType[record["BinaryArrayTypeEnum"]].name.endswith("Offset"):
                for bound in record["LowerBounds"]:
                    self.read_write_primitive(PrimitiveType.Int32, bound, mode="write")

            self.read_write_enum(
                BinaryType, BinaryType[record["TypeEnum"]], mode="write"
            )
            self.read_write_binary_type_info(
                BinaryType[record["TypeEnum"]],
                record["AdditionalTypeInfo"],
                mode="write",
            )

            for value in record["Values"]:
                self.read_write_record(value, mode="write", records=self.records)

    def handle_member_primitive_typed(self, record=None, mode="read"):
        """Handle single typed primitive members."""
        if mode == "read":
            primitive_type = self.read_write_enum(PrimitiveType, mode="read")
            val = self.read_write_primitive(primitive_type, mode="read")
            return {"PrimitiveTypeEnum": primitive_type.name, "Value": val}
        else:
            self.read_write_enum(
                PrimitiveType, PrimitiveType[record["PrimitiveTypeEnum"]], mode="write"
            )
            self.read_write_primitive(
                PrimitiveType[record["PrimitiveTypeEnum"]],
                record["Value"],
                mode="write",
            )

    def handle_member_reference(self, record=None, mode="read"):
        """Handle reference to an existing object by its ID."""
        id_ref = self.read_write_primitive(
            PrimitiveType.Int32,
            None if mode == "read" else record["IdRef"],
            mode,
        )
        if mode == "read":
            record_data = {"IdRef": id_ref}
            self.references.append(record_data)
            return record_data
        else:
            self.references.append(record)

    def handle_object_null(self, record=None, mode="read"):
        """Handle a single null reference."""
        return {}

    def handle_message_end(self, record=None, mode="read"):
        """Handle the end of the message/stream."""
        return {}

    def handle_binary_library(self, record=None, mode="read"):
        """Handle library references (ID/name)."""
        library_id = self.read_write_primitive(
            PrimitiveType.Int32,
            None if mode == "read" else record["LibraryId"],
            mode,
        )
        library_name = self.read_write_string(
            None if mode == "read" else record["LibraryName"],
            mode,
        )
        if mode == "read":
            return {
                "LibraryId": library_id,
                "LibraryName": library_name,
            }

    def handle_object_null_multiple_256(self, record=None, mode="read"):
        """Handle multiple null references (count up to 256)."""
        null_count = self.read_write_primitive(
            PrimitiveType.Byte,
            None if mode == "read" else record["NullCount"],
            mode,
        )
        if mode == "read":
            return {"NullCount": null_count}

    def handle_object_null_multiple(self, record=None, mode="read"):
        """Handle multiple null references (count as Int32)."""
        null_count = self.read_write_primitive(
            PrimitiveType.Int32,
            None if mode == "read" else record["NullCount"],
            mode,
        )
        if mode == "read":
            return {"NullCount": null_count}

    def handle_array_single_primitive(self, record=None, mode="read"):
        """Handle array of a single primitive type."""
        array_info = self.read_write_array_info(
            None if mode == "read" else record["ArrayInfo"],
            mode,
        )
        primitive_type = self.read_write_enum(
            PrimitiveType,
            None if mode == "read" else record["PrimitiveTypeEnum"],
            mode,
        )
        if mode == "read":
            values = [
                self.read_write_primitive(primitive_type, mode="read")
                for _ in range(array_info["Length"])
            ]
            return {
                "ArrayInfo": array_info,
                "PrimitiveTypeEnum": primitive_type.name,
                "Values": values,
            }
        else:
            for val in record["Values"]:
                self.read_write_primitive(
                    PrimitiveType[record["PrimitiveTypeEnum"]], val, mode="write"
                )

    def handle_array_single_object(self, record=None, mode="read"):
        """Handle array of single objects."""
        return self.handle_array_single(record, mode=mode)

    def handle_array_single_string(self, record=None, mode="read"):
        """Handle array of single strings."""
        return self.handle_array_single(record, mode=mode)

    def handle_array_single(self, record=None, mode="read"):
        """Generic handler for arrays of single elements (object/string)."""
        array_info = self.read_write_array_info(
            None if mode == "read" else record["ArrayInfo"],
            mode,
        )
        if mode == "read":
            values = self.read_write_array_values(array_info["Length"], mode="read")
            return {
                "ArrayInfo": array_info,
                "Values": values,
            }
        else:
            for val in record["Values"]:
                self.read_write_record(val, mode="write", records=self.records)

    def read_write_array_values(self, length: int, mode: str = "read"):
        """Read or write an array of 'length' objects/strings."""
        values = []
        i = 0
        while i < length:
            value = self.read_write_record(mode=mode, records=self.records)
            if isinstance(value, dict) and "NullCount" in value:
                i += value["NullCount"]
            else:
                i += 1
            if i > length:
                raise ValueError("Too many records in array.")
            values.append(value)
        return values

    def read_write_class_values(
        self,
        class_record: Dict[str, Any],
        value: Optional[Dict[str, Any]] = None,
        mode: str = "read",
    ):
        """Read or write a class's member values, referencing the metadata if needed."""
        if mode == "read":
            values = []

            # If we have a 'MetadataId', then we must find that parent's record
            if "MetadataId" in class_record:
                metadata_id = class_record["MetadataId"]
                class_record_with_metadata = self.get_parent_by_object_id(
                    metadata_id, self.records
                )
            else:
                class_record_with_metadata = class_record

            m_count = class_record_with_metadata["ClassInfo"]["MemberCount"]
            for i in range(m_count):
                mti = class_record_with_metadata["MemberTypeInfo"]
                bte = mti["BinaryTypeEnums"][i]
                btype = BinaryType[bte]
                additional_info = mti["AdditionalInfos"][i]

                val = self.read_write_value(btype, additional_info)
                values.append(val)

            class_record["Values"] = values

            # Store the object by its ID for later reference
            if "ClassInfo" in class_record:
                self.objects[class_record["ClassInfo"]["ObjectId"]] = class_record
            elif "ObjectId" in class_record:
                self.objects[class_record["ObjectId"]] = class_record

        else:
            # mode == "write"
            if ("MemberTypeInfo" not in class_record) and (
                "MetadataId" in class_record
            ):
                # If there's no direct MemberTypeInfo, get it from the parent
                metadata_id = class_record["MetadataId"]
                class_record_with_metadata = self.get_parent_by_object_id(
                    metadata_id, self.records
                )
                member_type_info = class_record_with_metadata["MemberTypeInfo"]
            else:
                member_type_info = class_record["MemberTypeInfo"]

            for i, val in enumerate(class_record["Values"]):
                bte = member_type_info["BinaryTypeEnums"][i]
                btype = BinaryType[bte]
                additional_info = member_type_info["AdditionalInfos"][i]
                self.read_write_value(btype, additional_info, val, mode="write")

    def read_write_value(
        self,
        binary_type: BinaryType,
        additional_info: Any,
        value: Any = None,
        mode: str = "read",
    ):
        """
        Custom read/write for a single member or item. Delegates to read_write_primitive
        or read_write_record based on the BinaryType.
        """
        if binary_type == BinaryType.Primitive:
            primitive_type = PrimitiveType[additional_info]
            return self.read_write_primitive(primitive_type, value, mode=mode)
        else:
            return self.read_write_record(value, mode=mode, records=self.records)


class UDLG:
    """
    High-level parser/encoder for UDLG files, built on top of NetSerializer.
    """

    def __init__(self, stream: BytesIO):
        self.serializer = NetSerializer(stream)
        self.header: bytes = b""
        self.records: List[Dict[str, Any]] = []

    def parse(self) -> Dict[str, Any]:
        """Decode (parse) the UDLG stream and return a dict with header and records."""
        self.header = self.serializer.read(24)
        while True:
            record = self.serializer.read_write_record(
                mode="read", records=self.records
            )
            # Remove any temporary records (placeholders)
            self.records = [r for r in self.records if "__temp_record" not in r]
            self.records.append(record)

            if record["RecordTypeEnum"] == "MessageEnd":
                break

        return {"Header": self.header.hex().upper(), "Records": self.records}

    def encode(self, data: Dict[str, Any]) -> None:
        """
        Encode the data back into the UDLG format using the NetSerializer.
        Write the resulting bytes to the stream.
        """
        self.serializer.write(bytes.fromhex(data["Header"]))
        for record in data["Records"]:
            self.serializer.read_write_record(
                record, mode="write", records=data["Records"]
            )


"""
The following CLI code uses Typer to handle:
- Decoding UDLG files to JSON (+ optional CSV extraction).
- Encoding JSON back to UDLG (+ optional CSV replacement).
- Merging existing translations into a new CSV (to avoid re-translation).
"""

app = typer.Typer(pretty_exceptions_enable=False)


def process_file(
    file_path: Path,
    output_path: Path,
    is_decode: bool,
    use_csv: bool = False,
    csv_data: List[List[str]] = None,
    include_file_path: bool = False,
):
    """
    Decode or encode a single file depending on 'is_decode'.
    Optionally extract or replace text using CSV data.
    """
    print(f'Processing "{file_path}"')

    if is_decode:
        # Decode .udlg into JSON
        with open(file_path, "rb") as f:
            udlg = UDLG(f)
            data = udlg.parse()

        # Extract CSV lines if requested
        if use_csv and csv_data is not None:
            data = extract_texts_to_csv(data, file_path, csv_data, include_file_path)

        with open(output_path, "w", encoding="utf-8") as out_json:
            json.dump(data, out_json, indent=2, ensure_ascii=False)

    else:
        # Encode JSON back into .udlg
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Replace CSV lines if requested
        if use_csv and csv_data is not None:
            data = replace_texts_from_csv(data, file_path, csv_data, include_file_path)

        output = BytesIO()
        udlg = UDLG(output)
        udlg.encode(data)

        with open(output_path, "wb") as out_file:
            out_file.write(output.getvalue())


def extract_texts_to_csv(
    data: Dict[str, Any],
    file_path: Path,
    csv_data: List[List[str]],
    include_file_path: bool = False,
) -> Dict[str, Any]:
    """
    Traverse the records looking for text that follows an "English" marker.
    Store these texts into csv_data if they don't already exist.
    Replaces newline characters with literal '\n' for CSV storage.
    """

    # Find object IDs that hold the "English" string
    english_object_ids = set()
    for record in data["Records"]:
        if "Values" in record:
            for val in record["Values"]:
                if (
                    isinstance(val, dict)
                    and val.get("RecordTypeEnum") == "BinaryObjectString"
                    and val.get("Value") == "English"
                    and "ObjectId" in val
                ):
                    english_object_ids.add(val["ObjectId"])

    for record in data["Records"]:
        if "Values" in record:
            previous_was_english = False
            previous_id_ref = None

            for val in record["Values"]:
                if not isinstance(val, dict):
                    previous_was_english = False
                    previous_id_ref = None
                    continue

                # If we see a reference, store it
                if val.get("RecordTypeEnum") == "MemberReference":
                    previous_id_ref = val.get("IdRef")
                    continue

                if val.get("RecordTypeEnum") == "BinaryObjectString":
                    current_text = val.get("Value")

                    # If the text is "English", set a flag
                    if current_text == "English":
                        previous_was_english = True
                        continue

                    # If the previous text was English or an English reference
                    # then we consider this text as translatable
                    if previous_was_english or (
                        previous_id_ref and previous_id_ref in english_object_ids
                    ):
                        processed_text = current_text.replace("\r\n", "\\n").replace(
                            "\n", "\\n"
                        )

                        # Build CSV row
                        row = [
                            processed_text,
                            # processed_text,
                            "",
                        ]  # [Original, Translation]
                        if include_file_path:
                            row.insert(0, str(file_path))

                        # Append only if it doesn't already exist in csv_data
                        if not any(processed_text == r[-2] for r in csv_data):
                            csv_data.append(row)

                        previous_was_english = False
                        previous_id_ref = None
                    else:
                        previous_was_english = False

    return data


def replace_texts_from_csv(
    data: Dict[str, Any],
    file_path: Path,
    csv_data: List[List[str]],
    include_file_path: bool = False,
) -> Dict[str, Any]:
    """
    Replace lines found in CSV (that follow an English marker in the data).
    Restores '\n' from the literal backslash representation in CSV into
    actual newlines.
    """

    # Build a dictionary of translations from the CSV
    # in the form translations[original_text] = new_text
    # If include_file_path is True, the CSV includes the file path in the first column.
    if include_file_path:
        translations = {
            row[-2].replace("\\n", "\r\n"): row[-1].replace("\\n", "\r\n")
            for row in csv_data
            if row[0] == str(file_path) and row[-2] != row[-1]
        }
    else:
        translations = {
            row[0].replace("\\n", "\r\n"): row[1].replace("\\n", "\r\n")
            for row in csv_data
            if row[0] != row[1]
        }

    # Find "English" object IDs
    english_object_ids = set()
    for record in data["Records"]:
        if "Values" in record:
            for val in record["Values"]:
                if (
                    isinstance(val, dict)
                    and val.get("RecordTypeEnum") == "BinaryObjectString"
                    and val.get("Value") == "English"
                ):
                    english_object_ids.add(val["ObjectId"])

    # Now replace text if the previous marker was English or a reference to English
    for record in data["Records"]:
        if "Values" in record:
            previous_was_english = False
            previous_id_ref = None

            for val in record["Values"]:
                if not isinstance(val, dict):
                    previous_was_english = False
                    previous_id_ref = None
                    continue

                if val.get("RecordTypeEnum") == "MemberReference":
                    previous_id_ref = val.get("IdRef")
                    continue

                if val.get("RecordTypeEnum") == "BinaryObjectString":
                    current_text = val.get("Value")

                    if current_text == "English":
                        previous_was_english = True
                        continue

                    if previous_was_english or (
                        previous_id_ref and previous_id_ref in english_object_ids
                    ):
                        # If there is a matching translation in the CSV, replace it
                        if current_text in translations:
                            val["Value"] = translations[current_text]

                        previous_was_english = False
                        previous_id_ref = None
                    else:
                        previous_was_english = False

    return data


@app.command()
def decode(
    input_path: Path = typer.Argument(..., help="Input UDLG file or folder"),
    output: Optional[Path] = typer.Option(
        None, "-o", "--output", help="Output JSON file or folder"
    ),
    extract_csv: bool = typer.Option(False, "-c", "--csv", help="Extract texts to CSV"),
    include_file_path: bool = typer.Option(
        False, "-f", "--include-file", help="Include file path in CSV"
    ),
):
    """
    Decode .udlg file(s) into JSON. Optionally extract text into CSV.
    """
    csv_data: List[List[str]] = []

    if input_path.is_file():
        output_path = output or input_path.with_suffix(".json")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        process_file(
            input_path, output_path, True, extract_csv, csv_data, include_file_path
        )
    elif input_path.is_dir():
        # If directory, decode all .udlg files inside
        output_dir = output or input_path.with_name(f"{input_path.name}_json")
        for file in input_path.glob("**/*.udlg"):
            out_file = output_dir / file.with_suffix(".json").relative_to(input_path)
            out_file.parent.mkdir(parents=True, exist_ok=True)
            process_file(file, out_file, True, extract_csv, csv_data, include_file_path)
    else:
        typer.echo(f"Error: {input_path} is not a valid file or directory")
        raise typer.Exit(code=1)

    if extract_csv:
        # Write CSV file
        csv_file = (output if output else input_path).with_suffix(".csv")
        csv_file.parent.mkdir(parents=True, exist_ok=True)
        with csv_file.open("w", newline="", encoding="utf-8") as csvfile:
            csv_writer = csv_module.writer(csvfile)
            # Write headers
            headers = ["Original", "Translation"]
            if include_file_path:
                headers.insert(0, "File")
            csv_writer.writerow(headers)
            csv_writer.writerows(csv_data)


@app.command()
def encode(
    input_path: Path = typer.Argument(..., help="Input JSON file or folder"),
    output: Optional[Path] = typer.Option(
        None, "-o", "--output", help="Output UDLG file or folder"
    ),
    csv_file: Optional[Path] = typer.Option(
        None, "-c", "--csv", help="CSV file with texts"
    ),
    include_file_path: bool = typer.Option(
        False, "-f", "--include-file", help="CSV includes file path"
    ),
):
    """
    Encode JSON back into .udlg format, optionally applying CSV translations.
    """
    csv_data: List[List[str]] = []

    # If a CSV is provided, load it
    if csv_file:
        if not csv_file.exists():
            typer.echo(f"CSV file {csv_file} does not exist.")
            raise typer.Exit(code=1)

        with csv_file.open("r", newline="", encoding="utf-8") as csvfile:
            csv_reader = csv_module.reader(csvfile)
            next(csv_reader, None)  # Skip header row
            csv_data = list(csv_reader)

    if input_path.is_file():
        output_path = output or input_path.with_suffix(".udlg")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        process_file(
            input_path, output_path, False, bool(csv_file), csv_data, include_file_path
        )
    elif input_path.is_dir():
        output_dir = output or input_path.with_name(f"{input_path.name}_udlg")
        output_dir.mkdir(parents=True, exist_ok=True)
        for file in input_path.glob("**/*.json"):
            out_file = output_dir / file.with_suffix(".udlg").relative_to(input_path)
            out_file.parent.mkdir(parents=True, exist_ok=True)
            process_file(
                file, out_file, False, bool(csv_file), csv_data, include_file_path
            )
    else:
        typer.echo(f"Error: {input_path} is not a valid file or directory")
        raise typer.Exit(code=1)


@app.command()
def merge_csv(
    base_csv: Path = typer.Argument(..., help="Base CSV file with older translations"),
    new_csv: Path = typer.Argument(
        ..., help="New CSV extracted from updated game files"
    ),
    merged_csv: Path = typer.Argument(..., help="Output CSV with merged translations"),
):
    """
    Merge existing translations from the base CSV into the new CSV.
    - If the original text has not changed, carry over the translation.
    - If it has changed, keep the new text as original and leave translation identical to original.
    """

    if not base_csv.exists():
        typer.echo(f"Error: Base CSV {base_csv} does not exist.")
        raise typer.Exit(code=1)
    if not new_csv.exists():
        typer.echo(f"Error: New CSV {new_csv} does not exist.")
        raise typer.Exit(code=1)

    with base_csv.open("r", encoding="utf-8", newline="") as f_base:
        base_reader = csv_module.reader(f_base)
        base_header = next(base_reader, [])
        base_rows = list(base_reader)

    with new_csv.open("r", encoding="utf-8", newline="") as f_new:
        new_reader = csv_module.reader(f_new)
        new_header = next(new_reader, [])
        new_rows = list(new_reader)

    # In many workflows, the structure of the CSV is:
    #   Possibly: [FilePath], Original, Translation
    # or simply: [Original, Translation]
    # We'll detect which approach is in use by checking header length.

    # Create dict for base CSV with key=original_text, value=translation
    # If it includes a file column, the original text is at -2 index, the translation at -1 index.
    # If not, the original and translation are at indices 0 and 1 respectively.

    file_included = "File" in base_header or "File" in new_header

    def get_original(row: List[str]) -> str:
        if file_included:
            return row[-2]
        return row[0]

    def get_translation(row: List[str]) -> str:
        if file_included:
            return row[-1]
        return row[1]

    # Build dictionary from base_csv
    base_dict = {}
    for row in base_rows:
        if len(row) < 2:
            continue
        base_dict[get_original(row)] = get_translation(row)

    merged_data = []
    # Build the merged CSV data
    for row in new_rows:
        if len(row) < 2:
            merged_data.append(row)
            continue

        original_text = get_original(row)
        # If the original_text is found in the base_dict, we check if the new row's original text
        # is exactly the same as it was in the base file. If so, we carry over the translation.
        if original_text in base_dict:
            # Only replace translation if it is the same original text
            # (meaning it hasn't changed from the old content).
            # If the new file changed the original text, we do NOT re-use.
            # Here we assume "unchanged" means the text is an exact match (case-sensitive).
            old_translation = base_dict[original_text]

            # Replace in the merged row
            if file_included:
                row[-1] = old_translation
            else:
                row[1] = old_translation

        merged_data.append(row)

    # Write out merged result
    merged_csv.parent.mkdir(parents=True, exist_ok=True)
    with merged_csv.open("w", encoding="utf-8", newline="") as f_merged:
        writer = csv_module.writer(f_merged)
        # Write the header if present
        if base_header:
            writer.writerow(base_header)
        for row in merged_data:
            writer.writerow(row)

    typer.echo(f"Merged CSV saved to '{merged_csv}'")


if __name__ == "__main__":
    app()
