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

@docs_group('Classes')
class ContextPipeline(Generic[TCrawlingContext]):
    """Encapsulates the logic of gradually enhancing the crawling context with additional information and utilities.

    The enhancement is done by a chain of middlewares that are added to the pipeline after it's creation.
    """

    def __init__(
        self,
        *,
        _middleware: Callable[
            [TParentCrawlingContext],
            AsyncGenerator[TCrawlingContext, None],
        ]
        | None = None,
        _parent: ContextPipeline[TParentCrawlingContext] | None = None,
    ) -> None:
        self._middleware = _middleware
        self._parent = _parent
        self._middleware_action_and_cleanup = None

    def _middleware_chain(self) -> Generator[ContextPipeline[Any], None, None]:
        yield self

        if self._parent is not None:
            yield from self._parent._middleware_chain()  # noqa: SLF001

    async def __call__(
        self,
        root_crawling_context: BasicCrawlingContext,
        final_context_consumer: Callable[[TCrawlingContext], Awaitable[None]] | None = None,
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

    async def _run_middleware_action(self, root_crawling_context: BasicCrawlingContext) ->  tuple[TCrawlingContext, Any]:
        """Middleware is expected to return generator of 1 or 2 items. First is used to run, second to cleanup. More is ignored.
        That is too implicit and complex.

        TODO: Redo middleware to accept into compose/init MiddlewareCallback class, that has .run and .cleanup method to be explicit.
        -> Looks like natural use-case for context manager run = __aenter__, cleanup=__aexit__
        which would require to remember children and not parent
        natural order is then automatic and running pipeline is super simple. No need to reverse anything.
        root-action -> children-action1 -> children2-action -> user_action -> children2->cleanup -> children1->cleanup -> root-cleanup

        async def __call__(input_context, user_action):
            with self(input_context, user_action):
                print("Finished middleware and user call back. Start running cleanup.")

        async def __aenter__(input_context, user_action):
            self.context = await self.middleware.action()
                if self.child:
                    async with self.child(self.context)
                else:
                    await user_action(self.context)

        async def __aexit__(...):
            await self.middleware.cleanup()

        top level call in basic_crawler:
        async def __run_request_handler(self, context: BasicCrawlingContext) -> None:
            self._context_pipeline(context, self.router)
        """
        if self._parent is not None:
            parent_crawling_context = await self._parent._run_middleware_action(root_crawling_context=root_crawling_context)
        else:
            return root_crawling_context
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
            raise ContextPipelineInitializationError(e, crawling_context) from e

    async def _run_middleware_cleanup(self, root_context):
        if self._middleware is not None:
            try:
                await self._middleware_action_and_cleanup.__anext__()
            except StopAsyncIteration:  # noqa: PERF203
                pass
            except ContextPipelineInterruptedError as e:
                raise RuntimeError('Invalid state - pipeline interrupted in the finalization step') from e
            except Exception as e:
                raise ContextPipelineFinalizationError(e, root_context) from e
            else:
                raise RuntimeError('The middleware yielded more than once')
            await self._parent._run_middleware_cleanup(root_context)


    async def __call__Old(
        self,
        crawling_context: BasicCrawlingContext,
        final_context_consumer: Callable[[TCrawlingContext], Awaitable[None]],
    ) -> None:
        """Run a crawling context through the middleware chain and pipe it into a consumer function.

        Exceptions from the consumer function are wrapped together with the final crawling context.
        """
        chain = list(self._middleware_chain())
        cleanup_stack = list[AsyncGenerator]()

        try:
            for member in reversed(chain):
                if member._middleware:  # noqa: SLF001
                    middleware_instance = member._middleware(crawling_context)  # noqa: SLF001
                    try:
                        result = await middleware_instance.__anext__()
                    except SessionError:  # Session errors get special treatment
                        raise
                    except StopAsyncIteration as e:
                        raise RuntimeError('The middleware did not yield') from e
                    except ContextPipelineInterruptedError:
                        raise
                    except Exception as e:
                        raise ContextPipelineInitializationError(e, crawling_context) from e

                    crawling_context = result
                    cleanup_stack.append(middleware_instance)

            try:
                await final_context_consumer(cast(TCrawlingContext, crawling_context))
            except SessionError:  # Session errors get special treatment
                raise
            except Exception as e:
                raise RequestHandlerError(e, crawling_context) from e
        finally:
            for middleware_instance in reversed(cleanup_stack):
                try:
                    result = await middleware_instance.__anext__()
                except StopAsyncIteration:  # noqa: PERF203
                    pass
                except ContextPipelineInterruptedError as e:
                    raise RuntimeError('Invalid state - pipeline interrupted in the finalization step') from e
                except Exception as e:
                    raise ContextPipelineFinalizationError(e, crawling_context) from e
                else:
                    raise RuntimeError('The middleware yielded more than once')

    def compose(
        self: type[TParentCrawlingContext],
        middleware: Callable[
            [TParentCrawlingContext],
            AsyncGenerator[TCrawlingContext, None],
        ],
    ) -> ContextPipeline[TCrawlingContext]:
        """Add a middleware to the pipeline.

        The middleware should yield exactly once, and it should yield an (optionally) extended crawling context object.
        The part before the yield can be used for initialization and the part after it for cleanup.

        Returns:
            The extended pipeline instance, providing a fluent interface
        """
        return ContextPipeline[TCrawlingContext](
            _middleware=middleware,
            _parent=self,
        )
