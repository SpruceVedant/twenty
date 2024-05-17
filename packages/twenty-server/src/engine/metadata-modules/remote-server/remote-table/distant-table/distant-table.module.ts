import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';

import { RemoteServerEntity } from 'src/engine/metadata-modules/remote-server/remote-server.entity';
import { DistantTableResolver } from 'src/engine/metadata-modules/remote-server/remote-table/distant-table/distant-table.resolver';
import { DistantTableService } from 'src/engine/metadata-modules/remote-server/remote-table/distant-table/distant-table.service';
import { RemoteTableModule } from 'src/engine/metadata-modules/remote-server/remote-table/remote-table.module';
import { WorkspaceDataSourceModule } from 'src/engine/workspace-datasource/workspace-datasource.module';

@Module({
  imports: [
    WorkspaceDataSourceModule,
    TypeOrmModule.forFeature([RemoteServerEntity], 'metadata'),
    RemoteTableModule,
  ],
  providers: [DistantTableService, DistantTableResolver],
  exports: [DistantTableService],
})
export class DistantTableModule {}
