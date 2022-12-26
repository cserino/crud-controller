export class CrudError extends Error {
  statusCode: number;
  type: string;
  details?: any;
  isCrudError: true;
  constructor(
    message: string,
    type: string,
    statusCode: number = 500,
    details?: any
  ) {
    super(message);
    this.type = type;
    this.statusCode = statusCode;
    this.details = details;
    this.isCrudError = true;
  }

  static isCrudError(error: any): error is CrudError {
    return "isCrudError" in error && error.isCrudError;
  }

  static unauthorized() {
    return new CrudError("unauthorized", "unauthorized", 401);
  }

  static forbidden() {
    return new CrudError("forbidden", "forbidden", 403);
  }

  static notFound() {
    return new CrudError("not found", "not_found", 404);
  }
}

