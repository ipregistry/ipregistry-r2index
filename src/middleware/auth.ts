import { Context, Next } from 'hono';
import type { Env } from '../types';
import { Errors } from '../errors';

export async function authMiddleware(c: Context<{ Bindings: Env }>, next: Next) {
  const authHeader = c.req.header('Authorization');

  if (!authHeader) {
    return c.json(Errors.MISSING_AUTH_HEADER, 401);
  }

  const [scheme, token] = authHeader.split(' ');

  if (scheme !== 'Bearer' || !token) {
    return c.json(Errors.INVALID_AUTH_FORMAT, 401);
  }

  if (token !== c.env.API_TOKEN) {
    return c.json(Errors.INVALID_TOKEN, 403);
  }

  await next();
}
