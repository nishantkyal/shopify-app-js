import * as child_process from 'child_process';
import {promisify} from 'util';
import {resolve} from 'path';
import {DocumentClient} from 'aws-sdk';

import {
  batteryOfTests,
  wait,
} from '@shopify/shopify-app-session-storage-test-utils';
import {Session} from '@shopify/shopify-api';

import {AWSDynamoDBConnection} from '../awsdynamodb-connection';
import {AWSDynamoDBSessionStorage} from '../awsdynamodb';

import {v1_0_0SessionData} from './migration-test-data';

const exec = promisify(child_process.exec);

const dbURL = new URL('http://localhost:8000');

describe('AWSDynamoDBSessionStorage', () => {
  let containerId: string | undefined;
  let client: AWSDynamoDBConnection;

  beforeAll(async () => {
    const configPath = resolve(__dirname, './awsdynamodb.conf');
    const runCommand = await exec(
      `podman run -d -p 8000:8000 -v ${configPath}:/awsdynamodb.conf amazon/dynamodb-local dynamodb-server /awsdynamodb.conf`,
      {encoding: 'utf8'},
    );
    containerId = runCommand.stdout.trim();

    // Give the container a lot of time to set up since polling is ineffective with podman
    await wait(10000);

    client = new AWSDynamoDBConnection(dbURL.toString(), "shopify_session");
  });

  afterAll(async () => {
    if (containerId) await exec(`podman rm -f ${containerId}`);
  });

  describe('batteryOfTests', () => {
    let storage: AWSDynamoDBSessionStorage | undefined;
    beforeAll(async () => {
      // flush the DB
      // await client.flushDb();
      await initWithNonSessionData(client);

      storage = new AWSDynamoDBSessionStorage(dbURL);
      await storage.ready;
    });

    afterAll(async () => {
      await storage?.disconnect();
    });

    batteryOfTests(async () => storage!);
  });

  describe('migrateAddShopKeyToTrackSessionsByShop tests', () => {
    let storage: AWSDynamoDBSessionStorage | undefined;
    beforeAll(async () => {
      // flush the DB
      //await client.flushDb();
      await initWithNonSessionData(client);
      await initWithVersion1_0_0Data(client);
    });

    afterEach(async () => {
      await storage?.disconnect();
    });

    it('initially satisfies pre-migrateAddShopKeyToTrackSessionsByShop conditions', async () => {
      const shop1SessionIds = await client.get(
        'shopify_sessions_shop1.myshopify.com',
      );
      expect(shop1SessionIds).toBeNull();

      const shop2SessionIds = await client.get(
        'shopify_sessions_shop2.myshopify.com',
      );
      expect(shop2SessionIds).toBeNull();
    });

    it('migrates previous data to migrateAddShopKeyToTrackSessionsByShop', async () => {
      storage = new AWSDynamoDBSessionStorage(dbURL);
      await storage.ready;

      const shop1SessionIds = await client.get(
        'shopify_sessions_shop1.myshopify.com',
      );
      expect(shop1SessionIds).toBeDefined();
      const shop1SessionIdsArray = JSON.parse(shop1SessionIds as string);
      expect(shop1SessionIdsArray).toContain('shopify_sessions_abcde-12345');
      expect(shop1SessionIdsArray).toContain('shopify_sessions_abcde-67890');

      const shop2SessionIds = await client.get(
        'shopify_sessions_shop2.myshopify.com',
      );
      expect(shop2SessionIds).toBeDefined();
      const shop2SessionIdsArray = JSON.parse(shop2SessionIds as string);
      expect(shop2SessionIdsArray).toContain('shopify_sessions_vwxyz-12345');
      expect(shop2SessionIdsArray).toContain('shopify_sessions_vwxyz-67890');
    });

    it('manipulates migrateAddShopKeyToTrackSessionsByShop data structures correctly', async () => {
      storage = new AWSDynamoDBSessionStorage(dbURL);
      await storage.ready;

      await storage.deleteSession('abcde-12345');
      const shop1SessionIds = await client.get(
        'shopify_sessions_shop1.myshopify.com',
      );
      expect(shop1SessionIds).toBeDefined();
      const shop1SessionIdsArray = JSON.parse(shop1SessionIds as string);
      expect(shop1SessionIdsArray).not.toContain(
        'shopify_sessions_abcde-12345',
      );
      expect(shop1SessionIdsArray).toContain('shopify_sessions_abcde-67890');
      const shop1Sessions = await storage.findSessionsByShop(
        'shop1.myshopify.com',
      );
      expect(shop1Sessions).toHaveLength(1);
      expect(shop1Sessions[0].id).toBe('abcde-67890');

      const newSession = new Session({
        id: 'vwxyz-abcde',
        shop: 'shop2.myshopify.com',
        state: 'state',
        isOnline: false,
        scope: ['test_scope2'].toString(),
        accessToken: 'vwxyz-abcde-678',
      });
      await storage.storeSession(newSession);
      const shop2SessionIds = await client.get(
        'shopify_sessions_shop2.myshopify.com',
      );
      expect(shop2SessionIds).toBeDefined();
      const shop2SessionIdsArray = JSON.parse(shop2SessionIds as string);
      expect(shop2SessionIdsArray).toContain('shopify_sessions_vwxyz-12345');
      expect(shop2SessionIdsArray).toContain('shopify_sessions_vwxyz-67890');
      expect(shop2SessionIdsArray).toContain('shopify_sessions_vwxyz-abcde');
      const shop2Sessions = await storage.findSessionsByShop(
        'shop2.myshopify.com',
      );
      expect(shop2Sessions).toHaveLength(3);
    });
  });

  it(`one-time initialisation like migrations and table creations are run only once`, async () => {
    const storageClone1 = new AWSDynamoDBSessionStorage(dbURL);
    await storageClone1.ready;

    const storageClone2 = new AWSDynamoDBSessionStorage(dbURL);
    await storageClone2.ready;

    storageClone1.disconnect();
    storageClone2.disconnect();
  });
});

async function initWithNonSessionData(client: DocumentClient) {
  // Json type different from json array contrary to what is expected by Session.fromPropertyArray
  await client.set(
    'this_is_not_a_session_id',
    JSON.stringify('{"key": "with a random value"}'),
  );

  // Type json array as expected by the the Session.fromPropertyArray but does not contain session data
  await client.set('12563', '["Ford", "BMW", "Fiat"]');

  // Not a json value
  await client.set('256647', 'any random value really');
}

async function initWithVersion1_0_0Data(client: DocumentClient) {
  for (const session of v1_0_0SessionData) {
    await client.set(
      `shopify_sessions_${session.id}`,
      JSON.stringify(session.toPropertyArray()),
    );
  }
}
