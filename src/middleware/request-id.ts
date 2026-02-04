import { Context, Next } from 'hono';

export async function requestIdMiddleware(c: Context, next: Next) {
  const requestId = c.req.header('X-Request-ID') || crypto.randomUUID();
  c.set('requestId', requestId);
  c.header('X-Request-ID', requestId);
  await next();
}
