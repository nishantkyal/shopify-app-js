import {
  AbstractMigrationEngine,
  SessionStorageMigratorOptions,
  MigrationOperation,
} from '@shopify/shopify-app-session-storage';

import {AWSDynamoDBConnection} from './awsdynamodb-connection';

export class AWSDynamoDBSessionStorageMigrator extends AbstractMigrationEngine<
  AWSDynamoDBConnection,
  SessionStorageMigratorOptions
> {
  constructor(
    dbConnection: AWSDynamoDBConnection,
    opts: Partial<SessionStorageMigratorOptions> = {},
    migrations: MigrationOperation[],
  ) {
    // The name has already been decided with the last migration
    opts.migrationDBIdentifier = 'migrations';
    super(dbConnection, opts, migrations);
  }

  async initMigrationPersistence(): Promise<void> {
    // nothing to do here
    return Promise.resolve();
  }

  async hasMigrationBeenApplied(migrationName: string): Promise<boolean> {
    const migrations = await this.getMigrationRecords();
    const found = migrations.get(migrationName) ?? false;

    return Promise.resolve(found);
  }

  async saveAppliedMigration(migrationName: string): Promise<void> {
    const migrations = await this.getMigrationRecords();
    migrations.set(migrationName, true);

    this.connection.set(
      this.options.migrationDBIdentifier,
      JSON.stringify(Object.fromEntries(migrations)),
    );

    return Promise.resolve();
  }

  private async getMigrationRecords(): Promise<Map<string, boolean>> {
    const migrationsRecord = await this.connection.get(
      this.options.migrationDBIdentifier,
    );
    let migrations: Map<string, boolean> = new Map();
    if (migrationsRecord) {
      migrations = new Map(Object.entries(JSON.parse(migrationsRecord)));
    }

    return Promise.resolve(migrations);
  }
}
