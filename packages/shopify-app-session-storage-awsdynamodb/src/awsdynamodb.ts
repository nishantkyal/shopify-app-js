import {RedisClientOptions} from 'redis';
import {Session} from '@shopify/shopify-api';
import {
  SessionStorage,
  SessionStorageMigratorOptions,
  SessionStorageMigrator,
} from '@shopify/shopify-app-session-storage';

import {migrationList} from './migrations';
import {AWSDynamoDBConnection} from './awsdynamodb-connection';
import {AWSDynamoDBSessionStorageMigrator} from './awsdynamodb-migrator';

export interface AWSDynamoDBSessionStorageOptions extends RedisClientOptions {
  sessionKeyPrefix: string;
  onError?: (...args: any[]) => void;
  migratorOptions?: SessionStorageMigratorOptions;
}
const defaultAWSDynamoDBSessionStorageOptions: AWSDynamoDBSessionStorageOptions = {
  sessionKeyPrefix: 'shopify_sessions',
  migratorOptions: {
    migrationDBIdentifier: 'migrations',
  },
};

export class AWSDynamoDBSessionStorage implements SessionStorage {
  static withCredentials(
    host: string,
    opts: Partial<AWSDynamoDBSessionStorageOptions>,
  ) {
    return new AWSDynamoDBSessionStorage(
      new URL(
        `${host}`,
      ),
      opts,
    );
  }

  public readonly ready: Promise<void>;
  private internalInit: Promise<void>;
  private options: AWSDynamoDBSessionStorageOptions;
  private client: AWSDynamoDBConnection;
  private migrator: SessionStorageMigrator;

  constructor(dbUrl: URL, opts: Partial<AWSDynamoDBSessionStorageOptions> = {}) {
    this.options = {...defaultAWSDynamoDBSessionStorageOptions, ...opts};
    this.internalInit = this.init(dbUrl.toString());
    this.migrator = new AWSDynamoDBSessionStorageMigrator(
      this.client,
      this.options.migratorOptions,
      migrationList,
    );
    this.ready = this.migrator.applyMigrations(this.internalInit);
  }

  public async storeSession(session: Session): Promise<boolean> {
    await this.ready;

    await this.client.set(
      session.id,
      JSON.stringify(session.toPropertyArray()),
    );
    await this.addKeyToShopList(session);
    return true;
  }

  public async loadSession(id: string): Promise<Session | undefined> {
    await this.ready;

    let rawResult: any = await this.client.get(id);

    if (!rawResult) return undefined;
    rawResult = JSON.parse(rawResult);

    return Session.fromPropertyArray(rawResult);
  }

  public async deleteSession(id: string): Promise<boolean> {
    await this.ready;
    const session = await this.loadSession(id);
    if (session) {
      await this.removeKeyFromShopList(session.shop, id);
      await this.client.del(id);
    }
    return true;
  }

  public async deleteSessions(ids: string[]): Promise<boolean> {
    await this.ready;
    await Promise.all(ids.map((id) => this.deleteSession(id)));
    return true;
  }

  public async findSessionsByShop(shop: string): Promise<Session[]> {
    await this.ready;

    const idKeysArrayString = await this.client.get(shop);
    if (!idKeysArrayString) return [];

    const idKeysArray = JSON.parse(idKeysArrayString);
    const results: Session[] = [];
    for (const idKey of idKeysArray) {
      const rawResult = await this.client.get(idKey, false);
      if (!rawResult) continue;

      const session = Session.fromPropertyArray(JSON.parse(rawResult));
      results.push(session);
    }

    return results;
  }

  public async disconnect(): Promise<void> {
    await this.client.disconnect();
  }

  private async addKeyToShopList(session: Session) {
    const shopKey = session.shop;
    const idKey = this.client.generateFullKey(session.id);
    const idKeysArrayString = await this.client.get(shopKey);

    if (idKeysArrayString) {
      const idKeysArray = JSON.parse(idKeysArrayString);

      if (!idKeysArray.includes(idKey)) {
        idKeysArray.push(idKey);
        await this.client.set(shopKey, JSON.stringify(idKeysArray));
      }
    } else {
      await this.client.set(shopKey, JSON.stringify([idKey]));
    }
  }

  private async removeKeyFromShopList(shop: string, id: string) {
    const shopKey = shop;
    const idKey = this.client.generateFullKey(id);
    const idKeysArrayString = await this.client.get(shopKey);

    if (idKeysArrayString) {
      const idKeysArray = JSON.parse(idKeysArrayString);
      const index = idKeysArray.indexOf(idKey);

      if (index > -1) {
        idKeysArray.splice(index, 1);
        await this.client.set(shopKey, JSON.stringify(idKeysArray));
      }
    }
  }

  private async init(dbUrl: string) {
    this.client = new AWSDynamoDBConnection(
      endpoint,
      tableName,
      this.options,
      this.options.sessionKeyPrefix,
    );
    await this.client.connect();
  }
}
