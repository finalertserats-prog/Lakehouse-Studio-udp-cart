"""gRPC proto definitions for the v1.0 Go agent.

This directory holds ``.proto`` files only. No generated Python is committed
— the v1.0 session will run ``protoc`` (or ``grpc_tools.protoc``) as part of
its build step to produce ``agent_pb2.py`` + ``agent_pb2_grpc.py``
alongside the source ``.proto``.
"""
