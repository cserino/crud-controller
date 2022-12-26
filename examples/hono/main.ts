import { Hono, Context } from "hono";
import { StatusCode } from "hono/utils/http-status";
import { Kysely } from "kysely";
import { D1Dialect } from "kysely-d1";
import * as z from "zod";
import { ContextAdapter, CrudController } from "../../src";

type Bindings = {
  DB: D1Database;
};
type BoundEnv = { Bindings: Bindings };
type BoundContext = Context<any, BoundEnv, any>;

const databaseSchema = {
  users: z.object({
    user_id: z.string(),
    name: z.string().nullable(),
  }),
  things: z.object({
    thing_id: z.string(),
    user_id: z.string(),
    name: z.string(),
  }),
};
type Database = {
  [K in keyof typeof databaseSchema]: z.infer<(typeof databaseSchema)[K]>;
}

const app = new Hono<BoundEnv>();

/* The adapter connects your app's context -- whether that's a request from express, hapi, hono, or
   somewhere else. */
const adapter: ContextAdapter<Database, BoundContext> = {
  useDatabase(c) {
    return new Kysely<Database>({
      dialect: new D1Dialect({ database: c.env.DB }),
    });
  },
  get(c, key) {
    return c.get(key);
  },
  set(c, key, value) {
    return c.set(key, value);
  },
  getRequestParams(c) {
    return c.req.param();
  },
  getRequestBody(c) {
    return c.req.jsonData || c.req.json();
  },
  response(c, body, statusCode?) {
    return c.json(body, statusCode as StatusCode);
  },
};

const crud = new CrudController(databaseSchema, adapter);

/* The crud actions (index, create, show, update, destroy) are separate from
 * the response so that the actions can be composed before being ultimately
 * rendered into the response. */
app.get("/users", crud.collectionResponse(crud.index("users")));
app.post("/users", crud.response(crud.create("users", {
  attributes: {
    user_id: "uuid",
    name: "payload",
  },
})));
app.get("/users/:user_id", crud.response(crud.show("users")));
app.put("/users/:user_id", crud.response(crud.update("users", {
  attributes: {
    name: "payload",
  },
})));
app.put("/users/:user_id", crud.response(crud.destroy("users")));

/* Crud can be nested for related resources. in this case,
 * the :user_id param will be automatically picked up as a
 * scope in the nested endpoints. */
app.route("/users/:user_id/things", thingsApi());

function thingsApi() {
  const api = new Hono<BoundEnv>();

  api.get("/", crud.collectionResponse(crud.index("things")));
  api.post("/", crud.response(crud.create("things", {
    attributes: {
      thing_id: "uuid",
      user_id: "param",
      name: "payload",
    },
  })));
  api.get("/:thing_id", crud.response(crud.show("things")));
  api.put("/:thing_id", crud.response(crud.update("things", {
    attributes: {
      name: "payload",
    },
  })));
  api.put("/:thing_id", crud.response(crud.destroy("things")));
  return api;
}

