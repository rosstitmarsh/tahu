# To generate the base protobuf sparkplug_b Python library
protoc --proto_path=../../sparkplug_b/ --python_out=. --pyi_out=. ../../sparkplug_b/sparkplug_b.proto
