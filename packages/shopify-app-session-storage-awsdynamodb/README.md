# Session Storage Adapter for Redis

This package implements the `SessionStorage` interface that works with an instance of [AWS Dynamo DB](https://aws.amazon.com/dynamodb/).

```js
import {shopifyApp} from '@shopify/shopify-app-express';
import {AWSDynamoDBSessionStorage} from '@shopify/shopify-app-session-storage-awsdynamodb';

const shopify = shopifyApp({
  sessionStorage: new AWSDynamoDBSessionStorage(
    'redis://username:password@host/database',
  ),
  // ...
});

// OR

const shopify = shopifyApp({
  sessionStorage: AWSDynamoDBSessionStorage.withCredentials(
    'host.com',
    'thedatabase',
    'username',
    'password',
  ),
  // ...
});
```

If you prefer to use your own implementation of a session storage mechanism that is compatible with the `@shopify/shopify-app-express` package, see the [implementing session storage guide](../shopify-app-session-storage/implementing-session-storage.md).
