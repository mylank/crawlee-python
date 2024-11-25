from __future__ import annotations

from typing import Any, AsyncGenerator, Awaitable, Callable, Generator, Generic, cast

from typing_extensions import TypeVar

from crawlee._types import BasicCrawlingContext
from crawlee._utils.docs import docs_group
from crawlee.errors import (
    ContextPipelineFinalizationError,
    ContextPipelineInitializationError,
    ContextPipelineInterruptedError,
    RequestHandlerError,
    SessionError,
)

TCrawlingContext = TypeVar('TCrawlingContext', bound=BasicCrawlingContext, default=BasicCrawlingContext)
TParentCrawlingContext = TypeVar('TParentCrawlingContext', bound=BasicCrawlingContext, default=BasicCrawlingContext)
TChildrenCrawlingContext = TypeVar('TChildrenCrawlingContext', bound=BasicCrawlingContext, default=BasicCrawlingContext)

@docs_group('Classes')
class _ContextPipeline(Generic[TCrawlingContext]):
    """Encapsulates the logic of gradually enhancing the crawling context with additional information and utilities.

    The enhancement is done by a chain of middlewares that are added to the pipeline after it's creation.
    """

    def __init__(
        self,
        *,
        _middleware: Callable[
            [TParentCrawlingContext],
            AsyncGenerator[TCrawlingContext, None],
        ],
        _parent: _ContextPipeline[TParentCrawlingContext] | None = None,
    ) -> None:
        self._middleware = _middleware
        self._parent = _parent
        self._middleware_action_and_cleanup: AsyncGenerator[TCrawlingContext, None] | None= None


    async def __call__(
        self,
        root_crawling_context: BasicCrawlingContext,
        final_context_consumer: Callable[[TCrawlingContext], Awaitable[None]],
    ) -> None:
        """Run parent, then self, then final_context_consumer, then parent's cleanup, then own cleanup."""
        try:
            crawling_context = await self._run_middleware_action(root_crawling_context)

            try:
                await final_context_consumer(crawling_context)
            except SessionError:  # Session errors get special treatment
                raise
            except Exception as e:
                raise RequestHandlerError(e, crawling_context) from e

        finally:
            await self._run_middleware_cleanup(root_crawling_context)

    async def _run_middleware_action(self, root_crawling_context: BasicCrawlingContext) ->  TCrawlingContext:
        if self._parent is not None:
            parent_crawling_context = await self._parent._run_middleware_action(root_crawling_context=root_crawling_context)
        else:
            parent_crawling_context = root_crawling_context
        self._middleware_action_and_cleanup = self._middleware(parent_crawling_context)
        try:
            crawling_context = await self._middleware_action_and_cleanup.__anext__()
            return crawling_context

        except SessionError:  # Session errors get special treatment
            raise
        except StopAsyncIteration as e:
            raise RuntimeError('The middleware did not yield') from e
        except ContextPipelineInterruptedError:
            raise
        except Exception as e:
            raise ContextPipelineInitializationError(e, root_crawling_context) from e

    async def _run_middleware_cleanup(self, root_context: BasicCrawlingContext) -> None:
        if self._middleware is not None:
            try:
                if self._middleware_action_and_cleanup is not None:
                    await self._middleware_action_and_cleanup.__anext__()
            except StopAsyncIteration:  # noqa: PERF203
                pass
            except ContextPipelineInterruptedError as e:
                raise RuntimeError('Invalid state - pipeline interrupted in the finalization step') from e
            except Exception as e:
                raise ContextPipelineFinalizationError(e, root_context) from e
            else:
                raise RuntimeError('The middleware yielded more than once')
            if self._parent is not None:
                await self._parent._run_middleware_cleanup(root_context)


    def compose(
        self,
        middleware: Callable[
            [TCrawlingContext],
            AsyncGenerator[TChildrenCrawlingContext, None],
        ],
    ) -> _ContextPipeline[TChildrenCrawlingContext]:
        """Add a middleware to the pipeline.

        The middleware should yield exactly once, and it should yield an (optionally) extended crawling context object.
        The part before the yield can be used for initialization and the part after it for cleanup.

        Returns:
            The extended pipeline instance, providing a fluent interface
        """
        return _ContextPipeline[TChildrenCrawlingContext](
            _middleware=middleware,
            _parent=self,
        )


async def _no_action_middleware(crawling_context: BasicCrawlingContext) -> AsyncGenerator[
    BasicCrawlingContext, None]:
    yield crawling_context

class ContextPipeline(_ContextPipeline[BasicCrawlingContext]):
    def __init__(self) -> None:
        super().__init__(_middleware=_no_action_middleware, _parent=None)
