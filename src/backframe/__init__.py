from backframe.persistence import Persistence


class BackframeMiddleware:
    def __init__(self, app, persistence: Persistence):
        self.app = app
        self.persistence = persistence

    async def __call__(self, scope, receive, send):
        async with self.persistence.session():
            return await self.app(scope, receive, send)
