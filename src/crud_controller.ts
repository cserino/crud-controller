import type * as z from "zod";
import {
  Kysely,
  OnConflictBuilder,
  OnConflictDoNothingBuilder,
  OnConflictUpdateBuilder,
  SelectQueryBuilder,
  sql,
} from "kysely";
import { CrudError } from "./crud_error";

type AnyObject = Record<string, unknown>;

export interface ContextAdapter<Models extends ModelsObject, C> {
  useDatabase: (c: C) => Kysely<Models>;
  get(c: C, key: string): any;
  set(c: C, key: string, value: any): void;
  getRequestParams(c: C): AnyObject;
  getRequestBody(c: C): any;
  response(c: C, body: any, statusCode?: number): any;
}

type ModelsObject = Readonly<{ [model: string]: any }>;
type ModelOf<Models extends ModelsObject> = keyof Models & string;

type CrudDefinition<Models extends ModelsObject> = Readonly<{ [K in keyof Models]: z.AnyZodObject }>;

type AttributeFn<C> = (c: C) => any;
type RenderFn<Response extends AnyObject> = (
  record: Response
) => Promise<AnyObject>;

type AttributesPicker<
  Models extends ModelsObject,
  Model extends ModelOf<Models>,
  C
> = {
  [k in keyof Models[Model]]:
    | "null"
    | "default"
    | "uuid"
    | "get"
    | "arg"
    | "payload"
    | "param"
    | "inserted_at"
    | "updated_at"
    | AttributeFn<C>;
};

type WhereList<
  Models extends ModelsObject,
  Model extends ModelOf<Models>
> = Array<[lhs: keyof Models[Model], op: string, rhs: any]>;

type WhereOptions<
  Models extends ModelsObject,
  Model extends ModelOf<Models>,
  C
> =
  | WhereList<Models, Model>
  | ((c: C) => WhereList<Models, Model>);

export interface CrudControllerLoadOptions<
  Models extends ModelsObject,
  Model extends ModelOf<Models>,
  C
> {
  scopes?: string[];
  where?: WhereOptions<Models, Model, C>;
  query?: <O>(
    c: C,
    qb: SelectQueryBuilder<Models, Model, O>
  ) => SelectQueryBuilder<Models, Model, O>;
  as?: string;
}

export type CrudControllerIndexOptions<
  Models extends ModelsObject,
  Model extends ModelOf<Models>,
  C
> = CrudControllerLoadOptions<Models, Model, C>;

export interface CrudControllerCreateOptions<
  Models extends ModelsObject,
  Model extends ModelOf<Models>,
  C
> {
  attributes: AttributesPicker<Models, Model, C>;
  onConflict?: (
    builder: OnConflictBuilder<Models, Model>
  ) =>
    | OnConflictDoNothingBuilder<Models, Model>
    | OnConflictUpdateBuilder<Models, Model>;
}

export type CrudControllerShowOptions<
  Models extends ModelsObject,
  Model extends ModelOf<Models>,
  C
> = CrudControllerLoadOptions<Models, Model, C>;

export interface CrudControllerUpdateOptions<
  Models extends ModelsObject,
  Model extends ModelOf<Models>,
  C
> {
  where?: WhereOptions<Models, Model, C>;
  attributes: Partial<AttributesPicker<Models, Model, C>>;
}

export interface CrudControllerDestroyOptions<
  Models extends ModelsObject,
  Model extends ModelOf<Models>,
  C
> {
  where?: WhereOptions<Models, Model, C>;
  as?: string;
}

export interface ResponseOptions<Response extends AnyObject> {
  omit?: Array<keyof Response>;
  pick?: Array<keyof Response>;
  render?: RenderFn<Response>;
  statusCode?: number;
}

function createAttributesPickerFunction<
  Models extends ModelsObject,
  Model extends ModelOf<Models>,
  C
>(
  models: CrudDefinition<Models>,
  model: Model,
  adapter: ContextAdapter<Models, C>,
  picker: Partial<AttributesPicker<Models, Model, C>>
) {
  const schemaKeys: Record<string, true | undefined> = {};

  for (const key in picker) {
    const value = picker[key];
    switch (value) {
      case "payload":
        schemaKeys[key] = true;
        break;
    }
  }

  const schema = models[model].pick(schemaKeys);
  return async (c: C, payload: any, args?: any) => {
    const attributes = schema.parse(payload);

    for (const key in picker) {
      const pickerType = picker[key];
      switch (pickerType) {
        case "get":
          attributes[key] = adapter.get(c, key);
          break;
        case "arg":
          attributes[key] = args[key];
          break;
        case "param":
          attributes[key] = adapter.getRequestParams(c)?.[key];
          break;
        case "uuid":
          attributes[key] = crypto.randomUUID() as any;
          break;
        case "inserted_at":
          attributes[key] = new Date().toISOString() as any;
          break;
        case "updated_at":
          attributes[key] = new Date().toISOString() as any;
          break;
        case "null":
          attributes[key] = null;
          break;
        case "default":
          attributes[key] = sql.literal("DEFAULT");
          break;
        default:
          {
            if (typeof pickerType === "function") {
              const fnValue = (picker[key] as AttributeFn<C>)(c);
              if (typeof fnValue === "object" && "then" in fnValue) {
                attributes[key] = await fnValue;
              } else {
                attributes[key] = fnValue;
              }
            }
          }
          break;
      }
      if (typeof attributes[key] === "boolean") {
        attributes[key] = attributes[key] ? "1" : "0";
      }
    }

    return attributes;
  };
}

function pick<T>(obj: T, ...props: (keyof T)[]): Partial<T> {
  return props.reduce(function (result, prop) {
    result[prop] = obj[prop];
    return result;
  }, {} as Partial<T>);
}

function omit<T>(obj: T, ...props: (keyof T)[]): Partial<T> {
  const result = { ...obj };
  props.forEach(function (prop) {
    delete result[prop];
  });
  return result;
}

function singularize<
  Models extends ModelsObject,
  Model extends ModelOf<Models>,
  C
>(model: Model, loadOptions: CrudControllerLoadOptions<Models, Model, C>) {
  if (loadOptions.as) {
    return loadOptions.as;
  }
  return (model as string).slice(0, -1);
}

export class CrudController<Models extends ModelsObject, C> {
  models: CrudDefinition<Models>;
  adapter: ContextAdapter<Models, C>;

  constructor(
    models: CrudDefinition<Models>,
    adapter: ContextAdapter<Models, C>
  ) {
    this.models = models;
    this.adapter = adapter;
  }

  getScopes<Model extends ModelOf<Models>>(
    c: C,
    loadOptions: CrudControllerLoadOptions<Models, Model, C> = {}
  ) {
    let scopes = Object.entries(this.adapter.getRequestParams(c));
    if (loadOptions.scopes) {
      scopes = scopes.filter(([scope, _val]) =>
        loadOptions.scopes!.includes(scope)
      );
    }
    return scopes;
  }

  applyWhere<Model extends ModelOf<Models>>(
    c: C,
    where: WhereOptions<Models, Model, C>,
    query: any
  ) {
    if (typeof where === "function") {
      where = where(c);
    }
    for (const [lhs, op, rhs] of where) {
      query = query.where(lhs, op, rhs);
    }
    return query;
  }

  async executeIndex<Model extends ModelOf<Models>>(
    c: C,
    model: Model,
    indexOptions: CrudControllerIndexOptions<Models, Model, C> = {}
  ) {
    return this.index(model, indexOptions)(c);
  }

  index<Model extends ModelOf<Models>>(
    model: Model,
    indexOptions: CrudControllerIndexOptions<Models, Model, C> = {}
  ) {
    return async (c: C) => {
      const db = this.adapter.useDatabase(c);
      let query = db.selectFrom(model as string).selectAll();

      for (const [paramKey, paramValue] of this.getScopes(c, indexOptions)) {
        query = query.where(paramKey, "=", paramValue as any);
      }

      if (indexOptions.where) {
        query = this.applyWhere(c, indexOptions.where, query);
      }

      if (indexOptions.query) {
        query = indexOptions.query(c, query as any) as any;
      }
      console.log("index query", query.compile());

      const data = (await query.execute()) as Array<Models[Model]>;
      return {
        data,
        metadata: {
          total_count: 0,
        },
      };
    };
  }

  async executeCreate<Model extends ModelOf<Models>>(
    c: C,
    model: Model,
    createOptions: CrudControllerCreateOptions<Models, Model, C>
  ) {
    return this.create(model, createOptions)(c);
  }

  create<Model extends ModelOf<Models>>(
    model: Model,
    createOptions: CrudControllerCreateOptions<Models, Model, C>
  ) {
    const attributesPicker = createAttributesPickerFunction(
      this.models,
      model,
      this.adapter,
      createOptions.attributes
    );

    return async (
      c: C,
      args?: any
    ): Promise<Models[Model]> => {
      const db = this.adapter.useDatabase(c);

      const payload = this.adapter.getRequestBody(c);
      const attributes = await attributesPicker(c, payload, args);

      console.log("inserting", model, "attributes", attributes);

      const query = db
        .insertInto(model as string)
        .values(attributes as any)
        .if(!!createOptions.onConflict, (qb) =>
          qb.onConflict(createOptions.onConflict as any)
        )
        .returningAll();
      console.log("running query", query.compile());

      const data = await db
        .insertInto(model as string)
        .values(attributes as any)
        .if(!!createOptions.onConflict, (qb) =>
          qb.onConflict(createOptions.onConflict as any)
        )
        .returningAll()
        .executeTakeFirst();
      // pubsub.emit(`${model as string}.created`, data);

      return data as any;
    };
  }

  async load<Model extends ModelOf<Models>>(
    c: C,
    model: Model,
    loadOptions: CrudControllerLoadOptions<Models, Model, C> = {}
  ) {
    const db = this.adapter.useDatabase(c);

    let query = db.selectFrom(model as string).selectAll();

    for (const [paramKey, paramValue] of this.getScopes(c, loadOptions)) {
      query = query.where(paramKey, "=", paramValue as any);
    }

    if (loadOptions.where) {
      query = this.applyWhere(c, loadOptions.where, query);
    }

    console.log("load query", query.compile());

    const data = await query.executeTakeFirst();
    if (!data) {
      throw CrudError.notFound();
    }
    return data as Models[Model];
  }

  loader<Model extends ModelOf<Models>>(
    model: Model,
    loadOptions: CrudControllerLoadOptions<Models, Model, C> = {}
  ) {
    return async (c: C, next: () => Promise<any>) => {
      const data = await this.load(c, model, loadOptions);
      this.adapter.set(c, singularize(model, loadOptions), data);
      await next();
    };
  }

  async executeShow<Model extends ModelOf<Models>>(
    c: C,
    model: Model,
    showOptions: CrudControllerShowOptions<Models, Model, C> = {}
  ) {
    return this.show(model, showOptions)(c);
  }

  show<Model extends ModelOf<Models>>(
    model: Model,
    showOptions: CrudControllerShowOptions<Models, Model, C> = {}
  ) {
    return async (c: C) => {
      let data = this.adapter.get(c, singularize(model, showOptions));
      if (data === undefined) {
        data = await this.load(c, model, showOptions);
      }

      return data;
    };
  }

  async executeUpdate<Model extends ModelOf<Models>>(
    c: C,
    model: Model,
    updateOptions: CrudControllerUpdateOptions<Models, Model, C>
  ) {
    return this.update(model, updateOptions)(c);
  }

  update<Model extends ModelOf<Models>>(
    model: Model,
    updateOptions: CrudControllerUpdateOptions<Models, Model, C>
  ) {
    const attributesPicker = createAttributesPickerFunction(
      this.models,
      model,
      this.adapter,
      updateOptions.attributes
    );

    return async (
      c: C,
      args?: any
    ): Promise<Models[Model]> => {
      const db = this.adapter.useDatabase(c);

      let query = db.updateTable(model);

      for (const [paramKey, paramValue] of this.getScopes(c)) {
        query = query.where(paramKey, "=", paramValue as any);
      }

      if (updateOptions.where) {
        query = this.applyWhere(c, updateOptions.where, query);
      }

      const payload = this.adapter.getRequestBody(c);
      const updatedAttributes = await attributesPicker(c, payload, args);

      const data = await query
        .set(updatedAttributes as any)
        .returningAll()
        .executeTakeFirst();
      if (!data) {
        throw CrudError.notFound();
      }

      // pubsub.emit(`${model as string}.updated`, data);

      return data as any;
    };
  }

  executeDestroy<Model extends ModelOf<Models>>(
    c: C,
    model: Model,
    destroyOptions: CrudControllerDestroyOptions<Models, Model, C> = {}
  ) {
    return this.destroy(model, destroyOptions)(c);
  }
  destroy<Model extends ModelOf<Models>>(
    model: Model,
    destroyOptions: CrudControllerDestroyOptions<Models, Model, C> = {}
  ) {

    return async (c: C) => {
      const db = this.adapter.useDatabase(c);

      let record = this.adapter.get(c, singularize(model, destroyOptions));
      if (record === undefined) {
        record = await this.load(c, model, destroyOptions);
      }

      let query = db.deleteFrom(model);

      for (const [paramKey, paramValue] of this.getScopes(c)) {
        query = query.where(paramKey, "=", paramValue as any);
      }

      if (destroyOptions.where) {
        query = this.applyWhere(c, destroyOptions.where, query);
      }

      const data = await query.executeTakeFirst();
      if (data.numDeletedRows === 0n) {
        throw CrudError.notFound();
      }

      // pubsub.emit(`${model as string}.deleted`, {
      //   ...record,
      //   deleted_at: new Date().toISOString(),
      // });

      return {};
    };
  }

  collectionResponse<Response extends AnyObject>(
    dataFn: (
      c: C
    ) => Promise<{ data: Response[]; metadata: any }>,
    options: ResponseOptions<Response> = {}
  ) {
    return async (c: C) => {
      const { data: resultData, metadata } = await dataFn(c);

      const data = await Promise.all(
        resultData.map(async (row) => {
          let result: any = row;
          if (options.omit) {
            result = omit(result, ...options.omit);
          }
          if (options.pick) {
            result = pick(result, ...options.pick);
          }
          if (options.render) {
            result = await options.render(result);
          }
          return result;
        })
      );

      return this.adapter.response(c, { data, metadata }, options?.statusCode ?? 200);
    };
  }

  response<Response extends AnyObject>(
    dataFn: (c: C) => Promise<Response>,
    options: ResponseOptions<Response> = {}
  ) {
    return async (c: C) => {
      let data: any = await dataFn(c);
      if (options.omit) {
        data = omit(data, ...options.omit);
      }
      if (options.pick) {
        data = pick(data, ...options.pick);
      }
      if (options.render) {
        data = await options.render(data);
      }
      return this.adapter.response(c, { data }, options?.statusCode ?? 200);
    };
  }
}

