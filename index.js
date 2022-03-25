const AWS = require("aws-sdk");
const AmazonDaxClient = require("amazon-dax-client");
const _ = require("underscore");

let dynamodb = new AWS.DynamoDB({
  apiVersion: "2012-08-10"
});
dynamodb = new AWS.DynamoDB.DocumentClient({ service: dynamodb });
let client = dynamodb;


const awsfun = (type, method, params) => {
  return new Promise((resolve, reject) => {
    type[method](params, (err, data) => {
      if (err) {
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

const daxClientConnection = (endpoint) => {
  const dax = new AmazonDaxClient({
       endpoints: endpoint // Note : typeof endpoint is "array"
  });
  const daxClient = new AWS.DynamoDB.DocumentClient({ service: dax });
  return daxClient;
};

// Helper method to get combined data from every document
let getAllPageData = async (client, method, params, result, pageData) => {
  if (pageData["LastEvaluatedKey"]) {
    params["ExclusiveStartKey"] = pageData["LastEvaluatedKey"];
    let curPageData = await awsfun(client, method, params);
    result["Items"].push(...curPageData["Items"]);
    result = getAllPageData(client, method, params, result, curPageData);
    return result;
  } else {
    return result;
  }
};

let getalllimitdata = async (client, method, params, data, result, count, scannedcount, limit) => {
  if (data["LastEvaluatedKey"]) {
    params["ExclusiveStartKey"] = data["LastEvaluatedKey"];
    let curPageData = await awsfun(client, method, params);
    result["Items"].push(...curPageData["Items"]);
    data = curPageData;
    let key = data["LastEvaluatedKey"];
    count = count + data.Count;
    if (count < limit && key) {
      result = getalllimitdata(client, method, params, data, result, count, scannedcount, limit);
      return result;
    } else {
      return result;
    }
  }
};

// Helper method to get count of every document
let getAllDocumentDataCount = async (client, method, params, result, pageData) => {
  if (pageData["LastEvaluatedKey"]) {
    params["ExclusiveStartKey"] = pageData["LastEvaluatedKey"];
    let curPageData = await awsfun(client, method, params);
    result["Count"] = result["Count"] + curPageData["Count"];
    result = getAllDocumentDataCount(client, method, params, result, curPageData);
    return result;
  } else {
    return result;
  }
};

const removeEmpty = (obj) => {
  const o = JSON.parse(JSON.stringify(obj)); // Clone source oect.
  return _.chain(o).omit(_.isNull).
    omit(_.isUndefined).
    omit((value) => {
      if (_.isString(value)) {
        return value === "";
      } else if (_.isObject(value)) {
        return _.isEmpty(value);
      }
    }).
    value(); // Return new object.
};

exports.scan = async (tableName, query, ...theArgs) => {
  try {
    if (typeof tableName !== "string" && tableName !== "") {
      throw new Error("tableName should be string");
    }
    if (typeof query !== "object" || Object.entries(query).length === 0) {
      throw new Error("Key should be object");
    }
    let params = {
      TableName: tableName,
      ScanFilter: {}
    };
    let index, projection;
    for (let i = 0; i < theArgs.length; i++) {
      if (i === 0 && typeof theArgs[i] === "string") {
        index = theArgs[i];
      } else if (i === 1 && typeof theArgs[i] === "string") {
        index = theArgs[i];
      } else if (i === 1 && typeof theArgs[i] === "object") {
        projection = theArgs[i];
      } else if (i === 2 && typeof theArgs[i] === "object") {
        projection = theArgs[i];
      } else if (i === 3 && typeof theArgs[i] === "object") {
        projection = theArgs[i];
      }
    }
    if (typeof index !== "undefined") {
      params["IndexName"] = index;
    }
    if (typeof projection !== "undefined") {
      params["AttributesToGet"] = Object.keys(projection);
    }

    let keys = Object.keys(query);
    keys.forEach((item) => {
      params["ScanFilter"][item] = {
        ComparisonOperator: "EQ",
        AttributeValueList: [query[item]]
      };
    });
    let result = await awsfun(client, "scan", params);
    return result["Items"];
  } catch (e) {
    throw new Error(e.message);
  }
};

exports.find = async (tableName, query, ...theArgs) => {
  try {
    if (typeof tableName !== "string" && tableName !== "") {
      throw new Error("tableName should be string");
    }
    if (typeof query !== "object" || Object.entries(query).length === 0) {
      throw new Error("Key should be object");
    }
    let index, filter, projection, limit, lastEvaluatedKey;
    for (let i = 0; i < theArgs.length; i++) {
      if (i === 0 && typeof theArgs[i] === "string") {
        index = theArgs[i];
      } else if (i === 0 && typeof theArgs[i] === "object") {
        filter = theArgs[i];
      } else if (i === 1 && typeof theArgs[i] === "string") {
        index = theArgs[i];
      } else if (i === 1 && typeof theArgs[i] === "object") {
        projection = theArgs[i];
      } else if (i === 1 && typeof theArgs[i] === "number") {
        limit = theArgs[i];
      } else if (i === 2 && typeof theArgs[i] === "number") {
        limit = theArgs[i];
      } else if (i === 2 && typeof theArgs[i] === "object") {
        projection = theArgs[i];
      } else if (i === 2 && typeof theArgs[i] === "string") {
        lastEvaluatedKey = theArgs[i];
      } else if (i === 3 && typeof theArgs[i] === "object") {
        projection = theArgs[i];
      } else if (i === 3 && typeof theArgs[i] === "string") {
        index = theArgs[i];
      } else if (i === 4 && typeof theArgs[i] === "object") {
        lastEvaluatedKey = theArgs[i];
      }
    }

    let params = {
      TableName: tableName,
      KeyConditions: {}
    };

    if (lastEvaluatedKey) {
      params["ExclusiveStartKey"] = lastEvaluatedKey;
    }

    if (index) {
      params["IndexName"] = index;
    }
    if (filter) {
      let keys = Object.keys(filter);
      params["QueryFilter"] = {};
      keys.forEach((item) => {
        let fkey = "EQ";
        let fvalue = filter[item];
        if (typeof filter[item] === "object") {
          fkey = Object.keys(filter[item])[0];
          fvalue = filter[item][fkey];
        }
        params["QueryFilter"][item] = {
          ComparisonOperator: fkey,
          AttributeValueList: [fvalue]
        };
      });
    }
    if (projection) {
      params["AttributesToGet"] = Object.keys(projection);
    }
    if (limit) {
      params["Limit"] = limit;
    }

    let keys = Object.keys(query);
    keys.forEach((item) => {
      let fkey = "EQ";
      let fvalue = query[item];
      if (typeof query[item] === "object") {
        fkey = Object.keys(query[item])[0];
        fvalue = query[item][fkey];
      }
      params["KeyConditions"][item] = {
        ComparisonOperator: fkey,
        AttributeValueList: [fvalue]
      };
    });
    let result = await awsfun(client, "query", params);
    if (result["LastEvaluatedKey"]) {
      result["Items"].map((value) => {
        value.lastEvaluatedKey = result["LastEvaluatedKey"];
      });
    }
    return result["Items"];
  } catch (e) {
    throw new Error(e.message);
  }
};

exports.findPagination = async (tableName, query, ...theArgs) => {
  try {
    if (typeof tableName !== "string" && tableName !== "") {
      throw new Error("tableName should be string");
    }
    if (typeof query !== "object" || Object.entries(query).length === 0) {
      throw new Error("Key should be object");
    }
    let index, filter, projection, limit, lastEvaluatedKey;
    for (let i = 0; i < theArgs.length; i++) {
      if (i === 0 && typeof theArgs[i] === "string") {
        index = theArgs[i];
      } else if (i === 0 && typeof theArgs[i] === "object") {
        filter = theArgs[i];
      } else if (i === 1 && typeof theArgs[i] === "string") {
        index = theArgs[i];
      } else if (i === 1 && typeof theArgs[i] === "object") {
        projection = theArgs[i];
      } else if (i === 1 && typeof theArgs[i] === "number") {
        limit = theArgs[i];
      } else if (i === 2 && typeof theArgs[i] === "number") {
        limit = theArgs[i];
      } else if (i === 2 && typeof theArgs[i] === "object") {
        projection = theArgs[i];
      } else if (i === 2 && typeof theArgs[i] === "string") {
        lastEvaluatedKey = theArgs[i];
      } else if (i === 3 && typeof theArgs[i] === "object") {
        projection = theArgs[i];
      } else if (i === 3 && typeof theArgs[i] === "string") {
        index = theArgs[i];
      } else if (i === 4 && typeof theArgs[i] === "object") {
        lastEvaluatedKey = theArgs[i];
      }
    }

    let params = {
      TableName: tableName,
      KeyConditions: {}
    };

    if (lastEvaluatedKey) {
      params["ExclusiveStartKey"] = lastEvaluatedKey;
    }

    if (index) {
      params["IndexName"] = index;
    }
    if (filter) {
      let keys = Object.keys(filter);
      params["QueryFilter"] = {};
      keys.forEach((item) => {
        let fkey = "EQ";
        let fvalue = filter[item];
        if (typeof filter[item] === "object") {
          fkey = Object.keys(filter[item])[0];
          fvalue = filter[item][fkey];
        }
        params["QueryFilter"][item] = {
          ComparisonOperator: fkey,
          AttributeValueList: [fvalue]
        };
      });
    }
    if (projection) {
      params["AttributesToGet"] = Object.keys(projection);
    }
    if (limit) {
      params["Limit"] = limit;
    }
    let keys = Object.keys(query);
    keys.forEach((item) => {
      let fkey = "EQ";
      let fvalue = query[item];
      if (typeof query[item] === "object") {
        fkey = Object.keys(query[item])[0];
        fvalue = query[item][fkey];
      }
      params["KeyConditions"][item] = {
        ComparisonOperator: fkey,
        AttributeValueList: [fvalue]
      };
    });
    let result = await awsfun(client, "query", params);

    let count = result.Count;
    let scannedcount = result.ScannedCount;
    let data = result;

    if (count < limit && result["LastEvaluatedKey"]) {
      result = await getalllimitdata(client, "query", params, data, result, count, scannedcount, limit);
    }

    if (result["LastEvaluatedKey"]) {
      result["Items"].map((value) => {
        value.lastEvaluatedKey = result["LastEvaluatedKey"];
      });
    }
    return result["Items"];
  } catch (e) {
    throw new Error(e.message);
  }
};

// Finding all records from specified table and with required filter, projection, limit
exports.findAll = async (tableName, query, ...theArgs) => {
  try {
    if (typeof tableName !== "string" && tableName !== "") {
      throw new Error("TableName should be string");
    }
    if (typeof query !== "object" || Object.entries(query).length === 0) {
      throw new Error("Key should be object");
    }
    let projection, cacheParam, index, filter, cacheClient, cacheEndpoint, limit;
    for (let i = 0; i < theArgs.length; i++) {
      if (i === 0 && typeof theArgs[i] === "string") {
        index = theArgs[i];
      } else if (i === 0 && typeof theArgs[i] === "object") {
        filter = theArgs[i];
      } else if (i === 1 && typeof theArgs[i] === "string") {
        index = theArgs[i];
      } else if (i === 1 && typeof theArgs[i] === "object") {
        projection = theArgs[i];
      } else if (i === 2 && typeof theArgs[i] === "number") {
        limit = theArgs[i];
      } else if (i === 2 && typeof theArgs[i] === "object") {
        projection = theArgs[i];
      } else if (i === 3 && typeof theArgs[i] === "number") {
        limit = theArgs[i];
      } else if (i === 2 && typeof theArgs[i] === "boolean") {
        cacheParam = theArgs[i];
        cacheEndpoint = theArgs[i + 1];
      } else if (i === 3 && typeof theArgs[i] === "boolean") {
        cacheParam = theArgs[i];
        cacheEndpoint = theArgs[i + 1];
      }
    }

    let params = {
      TableName: tableName,
      KeyConditions: {}
    };
    if (index) {
      params["IndexName"] = index;
    }
    if (cacheParam === true) {
      cacheClient = daxClientConnection(cacheEndpoint);
      params["ConsistentRead"] = false;
    }
    if (filter) {
      let keys = Object.keys(filter);
      params["QueryFilter"] = {};
      keys.forEach((item) => {
        let fkey = "EQ";
        let fvalue = filter[item];
        if (typeof filter[item] === "object") {
          fkey = Object.keys(filter[item])[0];
          fvalue = filter[item][fkey];
        }
        params["QueryFilter"][item] = {
          ComparisonOperator: fkey,
          AttributeValueList: [fvalue]
        };
      });
    }
    if (projection) {
      params["AttributesToGet"] = Object.keys(projection);
    }
    if (limit) {
      params["Limit"] = limit;
    }
    let keys = Object.keys(query);
    keys.forEach((item) => {
      let fkey = "EQ";
      let fvalue = query[item];
      if (typeof query[item] === "object") {
        fkey = Object.keys(query[item])[0];
        fvalue = query[item][fkey];
      }
      params["KeyConditions"][item] = {
        ComparisonOperator: fkey,
        AttributeValueList: _.isArray(fvalue) ? fvalue : [fvalue]
      };
    });

    // Recursively fetching records via pagination using 'query' method with 'LastEvaluatedKey' and 'ExclusiveStartKey' primary key technique
    let result = cacheParam ? await awsfun(cacheClient, "query", params) : await awsfun(client, "query", params);
    let pageData = result;
    result = cacheParam ? await getAllPageData(cacheClient, "query", params, result, pageData) : await getAllPageData(client, "query", params, result, pageData);
    return result["Items"];
  } catch (e) {
    throw new Error(e.message);
  }
};

exports.findN = async (tableName, query, ...theArgs) => {
  try {
    if (typeof tableName !== "string" && tableName !== "") {
      throw new Error("tableName should be string");
    }
    if (typeof query !== "object" || Object.entries(query).length === 0) {
      throw new Error("Key should be object");
    }

    let projection;
    for (let i = 0; i < theArgs.length; i++) {
      if (i === 0 && typeof theArgs[i] === "object") {
        projection = theArgs[i];
      }
    }

    let params = {
      RequestItems: {}
    };
    params["RequestItems"][tableName] = {};
    let keys = Object.keys(query);
    let tempQuery = query[keys[0]]["IN"].map((x) => {
      let y = {};
      y[keys[0]] = x;
      return y;
    });
    params["RequestItems"][tableName] = {
      Keys: tempQuery
    };
    if (projection) {
      params["RequestItems"][tableName]["AttributesToGet"] = Object.keys(projection);
    }
    let result = await awsfun(client, "batchGet", params);
    return result["Responses"][tableName];
  } catch (e) {
    throw new Error(e.message);
  }
};

exports.findOne = async (tableName, query, ...theArgs) => {
  try {
    let projection, cacheParam, index, filter, cacheClient, cacheEndpoint;
    for (let i = 0; i < theArgs.length; i++) {
      if (i === 0 && typeof theArgs[i] === "object") {
        projection = theArgs[i];
      } else if (i === 1 && typeof theArgs[i] === "object") {
        filter = theArgs[i];
      } else if (i === 0 && typeof theArgs[i] === "boolean") {
        cacheParam = theArgs[i];
        cacheEndpoint = theArgs[i + 1];
      } else if (i === 1 && typeof theArgs[i] === "boolean") {
        cacheParam = theArgs[i];
        cacheEndpoint = theArgs[i + 1];
      } else if (i === 0 && typeof theArgs[i] === "string") {
        index = theArgs[i];
      } else if (i === 1 && typeof theArgs[i] === "string") {
        index = theArgs[i];
      }
    }

    let params = {
      Key: query,
      TableName: tableName
    };

    if (filter) {
      let keys = Object.keys(filter);
      params["QueryFilter"] = {};
      keys.forEach((item) => {
        let fkey = "EQ";
        let fvalue = filter[item];
        if (typeof filter[item] === "object") {
          fkey = Object.keys(filter[item])[0];
          fvalue = filter[item][fkey];
        }
        params["QueryFilter"][item] = {
          ComparisonOperator: fkey,
          AttributeValueList: [fvalue]
        };
      });
    }

    if (index) {
      params["IndexName"] = index;
    }

    if (cacheParam === true) {
      cacheClient = daxClientConnection(cacheEndpoint);
      params["ConsistentRead"] = false;
    }
    if (typeof projection !== "undefined") {
      params["AttributesToGet"] = Object.keys(projection);
    }
    let result = cacheParam ? await awsfun(cacheClient, "get", params) : await awsfun(client, "get", params);
    return result["Item"];
  } catch (e) {
    throw new Error(e.message);
  }
};

exports.updateOne = async (tableName, query, update, ...theArgs) => {
  try {
    // ReturnValues: NONE | ALL_OLD | UPDATED_OLD | ALL_NEW | UPDATED_NEW,
    // {'$set' : {},$inc : {}}

    let returnvalues, filter, index;
    for (let i = 0; i < theArgs.length; i++) {
      if (i === 0 && typeof theArgs[i] === "string") {
        returnvalues = theArgs[i];
      } else if (i === 1 && typeof theArgs[i] === "string") {
        index = theArgs[i];
      } else if (i === 0 && typeof theArgs[i] === "object") {
        filter = theArgs[i];
      }
    }

    let params = {
      Key: removeEmpty(query),
      TableName: tableName,
      AttributeUpdates: {},
      ReturnValues: typeof returnvalues === "undefined" ? "NONE" : returnvalues
    };

    if (index) {
      params["IndexName"] = index;
    }

    if (filter) {
      let keys = Object.keys(filter);
      params["QueryFilter"] = {};
      keys.forEach((item) => {
        let fkey = "EQ";
        let fvalue = filter[item];
        if (typeof filter[item] === "object") {
          fkey = Object.keys(filter[item])[0];
          fvalue = filter[item][fkey];
        }
        params["QueryFilter"][item] = {
          ComparisonOperator: fkey,
          AttributeValueList: [fvalue]
        };
      });
    }

    if (update["$set"]) {
      let keys = Object.keys(removeEmpty(update["$set"]));
      keys.forEach((item) => {
        params["AttributeUpdates"][item] = {
          Action: "PUT",
          Value: update["$set"][item]
        };
      });
    }
    if (update["$inc"]) {
      let keys = Object.keys(removeEmpty(update["$inc"]));
      keys.forEach((item) => {
        params["AttributeUpdates"][item] = {
          Action: "ADD",
          Value: update["$inc"][item]
        };
      });
    }
    let result = await awsfun(client, "update", params);
    return result["Attributes"];
  } catch (e) {
    throw new Error(e.message);
  }
};

exports.insertOne = async (tableName, query, ...theArgs) => {
  try {
    // ReturnValues: NONE | ALL_OLD | UPDATED_OLD | ALL_NEW | UPDATED_NEW,
    // {'$set' : {},$inc : {}}

    let returnvalues;
    for (let i = 0; i < theArgs.length; i++) {
      if (i === 0 && typeof theArgs[i] === "string") {
        returnvalues = theArgs[i];
      }
    }

    let params = {
      Item: removeEmpty(query),
      TableName: tableName,
      ReturnValues: typeof returnvalues === "undefined" ? "NONE" : returnvalues
    };
    let result = await awsfun(client, "put", params);
    return result["Attributes"];
  } catch (e) {
    throw new Error(e.message);
  }
};

exports.countDocuments = async (tableName, query, ...theArgs) => {
  try {
    if (typeof tableName !== "string" && tableName !== "") {
      throw new Error("tableName should be string");
    }
    if (typeof query !== "object" || Object.entries(query).length === 0) {
      throw new Error("Key should be object");
    }
    let index, filter;
    for (let i = 0; i < theArgs.length; i++) {
      if (i === 0 && typeof theArgs[i] === "string") {
        index = theArgs[i];
      } else if (i === 0 && typeof theArgs[i] === "object") {
        filter = theArgs[i];
      } else if (i === 1 && typeof theArgs[i] === "string") {
        index = theArgs[i];
      }
    }

    let params = {
      TableName: tableName,
      KeyConditions: {},
      Select: "COUNT"
    };
    if (index) {
      params["IndexName"] = index;
    }
    if (filter) {
      let keys = Object.keys(filter);
      params["QueryFilter"] = {};
      keys.forEach((item) => {
        let fkey = "EQ";
        let fvalue = filter[item];
        if (typeof filter[item] === "object") {
          fkey = Object.keys(filter[item])[0];
          fvalue = filter[item][fkey];
        }
        params["QueryFilter"][item] = {
          ComparisonOperator: fkey,
          AttributeValueList: [fvalue]
        };
      });
    }
    let keys = Object.keys(query);
    keys.forEach((item) => {
      let fkey = "EQ";
      let fvalue = query[item];
      if (typeof query[item] === "object") {
        fkey = Object.keys(query[item])[0];
        fvalue = query[item][fkey];
      }
      params["KeyConditions"][item] = {
        ComparisonOperator: fkey,
        AttributeValueList: [fvalue]
      };
    });
    var result = await awsfun(client, "query", params);
    let pageData = result;
    // Get count of all documents recursively with pagination technique
    result = await getAllDocumentDataCount(client, "query", params, result, pageData);
    return result["Count"];
  } catch (e) {
    throw new Error(e.message);
  }
};

exports.remove = async (tableName, query) => {
  try {
    let params = {
      TableName: tableName,
      Key: query
    };
    let result = await awsfun(client, "delete", params);
    return result["Attributes"];
  } catch (e) {
    throw new Error(e.message);
  }
};

exports.insertMany = async (tableName, query) => {
  try {
    let params = {
      RequestItems: {}
    };
    params["RequestItems"][tableName] = query.map((x) => {
      let obj = {};
      obj["PutRequest"] = {};
      obj["PutRequest"]["Item"] = removeEmpty(x);
      return obj;
    });
    let result = await awsfun(client, "batchWrite", params);
    return result;
  } catch (e) {
    throw Error(e);
  }
};

exports.core = async (option, query) => {
  try {
    let result = await awsfun(client, option, query);
    return result;
  } catch (e) {
    throw new Error(e.message);
  }
};

exports.corepagination = async (option, query, cacheParam, limit) => {
  try {
    let params = query;
    let result = await awsfun(client, option, params);
    let count = result.Count;
    let scannedcount = result.ScannedCount;
    let data = result;

    if (count < limit && result["LastEvaluatedKey"]) {
      result = await getalllimitdata(client, "query", params, data, result, count, scannedcount, limit);
    }

    if (result["LastEvaluatedKey"]) {
      result["Items"].map((value) => {
        value.lastEvaluatedKey = result["LastEvaluatedKey"];
      });
    }
    return result["Items"];
  } catch (e) {
    throw new Error(e.message);
  }
};
