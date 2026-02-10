import { Context, Next } from 'hono';
import type { Env, Variables } from '../types';

export async function d1SessionMiddleware(c: Context<{ Bindings: Env; Variables: Variables }>, next: Next) {
  const bookmark = c.req.header('X-D1-Bookmark');
  const session = bookmark
    ? c.env.D1.withSession(bookmark)
    : c.env.D1.withSession('first-unconstrained');
  c.set('db', session as unknown as D1Database);

  await next();

  const newBookmark = session.getBookmark();
  if (newBookmark) {
    c.header('X-D1-Bookmark', newBookmark);
  }
}
