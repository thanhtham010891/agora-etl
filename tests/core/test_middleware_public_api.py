from __future__ import annotations

import importlib


def test_middleware_package_reexports_public_api() -> None:
    module = importlib.import_module("agora.core.middleware")

    expected_names = {
        "BatchFilterMiddleware",
        "BatchMapMiddleware",
        "BufferedSubmitMiddleware",
        "DrainableBufferedMiddleware",
        "DrainablePipelinedBatchMiddleware",
        "FilterMiddleware",
        "PipeableMiddleware",
        "MapMiddleware",
        "Middleware",
        "MiddlewareChain",
        "MiddlewareDataPlane",
        "MiddlewareFailure",
        "MiddlewareModeSpec",
        "MiddlewareProcessResult",
        "PipelinedBatchMiddleware",
        "PipelinedBatchStageSpec",
        "RetryMiddleware",
        "RouteMiddleware",
        "is_buffered_submit_middleware",
        "is_drainable_buffered_middleware",
        "is_drainable_pipelined_batch_middleware",
        "is_pipelined_batch_middleware",
    }

    for name in expected_names:
        assert hasattr(module, name), f"agora.core.middleware is missing public export {name}"


def test_middleware_core_import_path_remains_stable() -> None:
    from agora.core.middleware import Middleware, MiddlewareChain, RouteMiddleware

    module = importlib.import_module("agora.core.middleware")

    assert module.Middleware is Middleware
    assert module.MiddlewareChain is MiddlewareChain
    assert module.RouteMiddleware is RouteMiddleware
