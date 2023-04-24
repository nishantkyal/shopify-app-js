import {Config, DynamoDB} from 'aws-sdk';
import {DBConnection} from '@shopify/shopify-app-session-storage';

export class AWSDynamoDBConnection implements DBConnection {
  sessionStorageIdentifier: string;
  private client: DynamoDB.DocumentClient;
  private tableName:string;

  constructor(
    endpoint:string,
    tableName: string,
    keyPrefix: string
  ) {
    this.tableName = tableName;
    this.sessionStorageIdentifier = keyPrefix;
    this.init(endpoint);
  }

  query(_query: string, _params: any[]): Promise<any[]> {
    throw new Error('Method not implemented. Use get(string, boolean) instead');
  }

  async connect(): Promise<void> {
  }

  async disconnect(): Promise<void> {
  }

  async keys(name: string): Promise<any> {
    return this.client.scan({
      TableName: this.tableName
    });
  }

  async set(baseKey: string, value: any, addKeyPrefix = true) {
    value.id = baseKey;
    await this.client.put({
      TableName: this.tableName,
      Item: value
    });
  }

  async del(baseKey: string, addKeyPrefix = true): Promise<any> {
    return this.client.delete({
      TableName: this.tableName,
      Key: {
        id: baseKey
      }
    });
  }

  async get(baseKey: string, addKeyPrefix = true): Promise<any> {
    return this.client.get({
      TableName: this.tableName,
      Key: {
        id: baseKey
      }
    });
  }

  generateFullKey(name: string): string {
    return `${this.sessionStorageIdentifier}_${name}`;
  }

  private buildKey(baseKey: string, addKeyPrefix: boolean): string {
    return addKeyPrefix ? this.generateFullKey(baseKey) : baseKey;
  }

  private init(endpoint: string) {
    this.client = new DynamoDB.DocumentClient({
      endpoint: endpoint
    });
  }
}
