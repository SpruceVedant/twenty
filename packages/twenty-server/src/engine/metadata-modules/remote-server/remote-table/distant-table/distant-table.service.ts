import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';

import { EntityManager, Repository } from 'typeorm';
import { v4 } from 'uuid';

import {
  RemoteServerEntity,
  RemoteServerType,
} from 'src/engine/metadata-modules/remote-server/remote-server.entity';
import { WorkspaceDataSourceService } from 'src/engine/workspace-datasource/workspace-datasource.service';
import { DistantTables } from 'src/engine/metadata-modules/remote-server/remote-table/distant-table/types/distant-table';
import { STRIPE_DISTANT_TABLES } from 'src/engine/metadata-modules/remote-server/remote-table/distant-table/util/stripe-distant-tables.util';
import { PostgresTableSchemaColumn } from 'src/engine/metadata-modules/remote-server/types/postgres-table-schema-column';
import {
  DistantTableUpdate,
  RemoteTableStatus,
} from 'src/engine/metadata-modules/remote-server/remote-table/dtos/remote-table.dto';
import { RemoteTableService } from 'src/engine/metadata-modules/remote-server/remote-table/remote-table.service';
import { RemoteTableEntity } from 'src/engine/metadata-modules/remote-server/remote-table/remote-table.entity';
import { getForeignTableColumnName } from 'src/engine/metadata-modules/remote-server/remote-table/utils/get-foreign-table-column-name.util';

@Injectable()
export class DistantTableService {
  constructor(
    private readonly workspaceDataSourceService: WorkspaceDataSourceService,
    private readonly remoteTableService: RemoteTableService,
    @InjectRepository(RemoteServerEntity, 'metadata')
    private readonly remoteServerRepository: Repository<
      RemoteServerEntity<RemoteServerType>
    >,
  ) {}

  public async findDistantTablesByServerId(id: string, workspaceId: string) {
    const remoteServer = await this.remoteServerRepository.findOne({
      where: {
        id,
        workspaceId,
      },
    });

    if (!remoteServer) {
      throw new NotFoundException('Remote server does not exist');
    }

    const currentRemoteTables =
      await this.remoteTableService.findRemoteTablesByServerId({
        remoteServerId: id,
        workspaceId,
      });

    const currentRemoteTableDistantNames = currentRemoteTables.map(
      (remoteTable) => remoteTable.distantTableName,
    );

    const distantTables = await this.fetchDistantTables(
      remoteServer,
      workspaceId,
    );

    if (currentRemoteTables.length === 0) {
      const distantTablesWithStatus = Object.keys(distantTables).map(
        (tableName) => ({
          name: tableName,
          schema: remoteServer.schema,
          status: currentRemoteTableDistantNames.includes(tableName)
            ? RemoteTableStatus.SYNCED
            : RemoteTableStatus.NOT_SYNCED,
        }),
      );

      return distantTablesWithStatus;
    }

    return this.getDistantTablesWithUpdates({
      remoteServerSchema: remoteServer.schema,
      workspaceId,
      remoteTables: currentRemoteTables,
      distantTables,
    });
  }

  public async getDistantTableColumns(
    remoteServer: RemoteServerEntity<RemoteServerType>,
    workspaceId: string,
    tableName: string,
  ): Promise<PostgresTableSchemaColumn[]> {
    const distantTables = await this.fetchDistantTables(
      remoteServer,
      workspaceId,
    );

    return distantTables[tableName];
  }

  public async fetchDistantTables(
    remoteServer: RemoteServerEntity<RemoteServerType>,
    workspaceId: string,
  ): Promise<DistantTables> {
    if (remoteServer.schema) {
      return this.getDistantTablesFromDynamicSchema(remoteServer, workspaceId);
    }

    return this.getDistantTablesFromStaticSchema(remoteServer);
  }

  private async getDistantTablesFromDynamicSchema(
    remoteServer: RemoteServerEntity<RemoteServerType>,
    workspaceId: string,
  ): Promise<DistantTables> {
    if (!remoteServer.schema) {
      throw new BadRequestException('Remote server schema is not defined');
    }

    const tmpSchemaId = v4();
    const tmpSchemaName = `${workspaceId}_${remoteServer.id}_${tmpSchemaId}`;

    const workspaceDataSource =
      await this.workspaceDataSourceService.connectToWorkspaceDataSource(
        workspaceId,
      );

    const distantTables = await workspaceDataSource.transaction(
      async (entityManager: EntityManager) => {
        await entityManager.query(`CREATE SCHEMA "${tmpSchemaName}"`);

        await entityManager.query(
          `IMPORT FOREIGN SCHEMA "${remoteServer.schema}" FROM SERVER "${remoteServer.foreignDataWrapperId}" INTO "${tmpSchemaName}"`,
        ); // LIMIT TO tableName

        const createdForeignTableNames = await entityManager.query(
          `SELECT table_name, column_name, data_type, udt_name FROM information_schema.columns WHERE table_schema = '${tmpSchemaName}'`,
        );

        await entityManager.query(`DROP SCHEMA "${tmpSchemaName}" CASCADE`);

        return createdForeignTableNames.reduce(
          (acc, { table_name, column_name, data_type, udt_name }) => {
            if (!acc[table_name]) {
              acc[table_name] = [];
            }

            acc[table_name].push({
              columnName: column_name,
              dataType: data_type,
              udtName: udt_name,
            });

            return acc;
          },
          {},
        );
      },
    );

    return distantTables;
  }

  private async getDistantTablesFromStaticSchema(
    remoteServer: RemoteServerEntity<RemoteServerType>,
  ): Promise<DistantTables> {
    switch (remoteServer.foreignDataWrapperType) {
      case RemoteServerType.STRIPE_FDW:
        return STRIPE_DISTANT_TABLES;
      default:
        throw new BadRequestException(
          `Type ${remoteServer.foreignDataWrapperType} does not have a static schema.`,
        );
    }
  }

  private async getDistantTablesWithUpdates({
    remoteServerSchema,
    workspaceId,
    remoteTables,
    distantTables,
  }: {
    remoteServerSchema: string;
    workspaceId: string;
    remoteTables: RemoteTableEntity[];
    distantTables: DistantTables;
  }) {
    const schemaPendingUpdates =
      await this.getSchemaUpdatesBetweenForeignAndDistantTables({
        workspaceId,
        remoteTables,
        distantTables,
      });

    const remoteTablesDistantNames = remoteTables.map(
      (remoteTable) => remoteTable.distantTableName,
    );

    const distantTablesWithUpdates = Object.keys(distantTables).map(
      (tableName) => ({
        name: tableName,
        schema: remoteServerSchema,
        status: remoteTablesDistantNames.includes(tableName)
          ? RemoteTableStatus.SYNCED
          : RemoteTableStatus.NOT_SYNCED,
        schemaPendingUpdates: schemaPendingUpdates[tableName],
      }),
    );

    const deletedTables = Object.entries(schemaPendingUpdates)
      .filter(([_tableName, updates]) =>
        updates.includes(DistantTableUpdate.TABLE_DELETED),
      )
      .map(([tableName, updates]) => ({
        name: tableName,
        schema: remoteServerSchema,
        status: RemoteTableStatus.SYNCED,
        schemaPendingUpdates: updates,
      }));

    return distantTablesWithUpdates.concat(deletedTables);
  }

  private async getSchemaUpdatesBetweenForeignAndDistantTables({
    workspaceId,
    remoteTables,
    distantTables,
  }: {
    workspaceId: string;
    remoteTables: RemoteTableEntity[];
    distantTables: DistantTables;
  }): Promise<{ [tablename: string]: DistantTableUpdate[] }> {
    const updates = {};

    for (const remoteTable of remoteTables) {
      const distantTable = distantTables[remoteTable.distantTableName];
      const tableName = remoteTable.distantTableName;

      if (!distantTable) {
        updates[tableName] = [DistantTableUpdate.TABLE_DELETED];
        continue;
      }

      const distantTableColumnNames = new Set(
        distantTable.map((column) =>
          getForeignTableColumnName(column.columnName),
        ),
      );
      const foreignTableColumnNames = new Set(
        (
          await this.remoteTableService.fetchTableColumns(
            workspaceId,
            remoteTable.localTableName,
          )
        ).map((column) => column.columnName),
      );

      const columnsAdded = [...distantTableColumnNames].filter(
        (columnName) => !foreignTableColumnNames.has(columnName),
      );

      const columnsDeleted = [...foreignTableColumnNames].filter(
        (columnName) => !distantTableColumnNames.has(columnName),
      );

      if (columnsAdded.length > 0) {
        updates[tableName] = [
          ...(updates[tableName] || []),
          DistantTableUpdate.COLUMNS_ADDED,
        ];
      }
      if (columnsDeleted.length > 0) {
        updates[tableName] = [
          ...(updates[tableName] || []),
          DistantTableUpdate.COLUMNS_DELETED,
        ];
      }
    }

    return updates;
  }
}
